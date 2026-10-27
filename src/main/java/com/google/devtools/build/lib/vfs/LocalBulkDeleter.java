// Copyright 2026 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.google.devtools.build.lib.vfs;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.common.util.concurrent.Uninterruptibles;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link BulkDeleter} that deletes paths under an exec root.
 *
 * <p>A file, symlink, missing path, or empty directory is one {@code remove}. A non-empty tree with
 * fewer than {@link #PARALLEL_CHILD_DIRECTORY_THRESHOLD} immediate child directories is deleted with
 * the ordinary serial {@link Path#deleteTree} walk. Trees with enough sibling subdirectories fan
 * out across a small, low-priority pool sized well below the host CPU count, so deletion does not
 * take cores away from concurrent actions.
 */
public final class LocalBulkDeleter implements BulkDeleter {
  /**
   * Immediate child directories required before the parallel path runs. Flat directories (0) and
   * single-root chains (1) stay on the serial walk.
   */
  @VisibleForTesting static final int PARALLEL_CHILD_DIRECTORY_THRESHOLD = 2;

  /**
   * Caps deletion threads so prepare-time deletes cannot monopolize the machine. Matches the idea
   * behind {@code AsynchronousTreeDeleter}: keep the pool small and low-priority while a build is
   * running. Uses at most one quarter of the host CPUs, and never more than 4.
   */
  private static final int POOL_SIZE =
      Math.max(1, Math.min(4, Runtime.getRuntime().availableProcessors() / 4));

  private static final ExecutorService POOL =
      Executors.newFixedThreadPool(
          POOL_SIZE,
          new ThreadFactoryBuilder()
              .setNameFormat("local-bulk-delete-%d")
              .setDaemon(true)
              .setPriority(Thread.MIN_PRIORITY)
              .build());

  private final Path execRoot;
  private final ExecutorService pool;
  /** Incremented once per child directory submitted to the pool; tests assert serial cases stay 0. */
  private final AtomicInteger parallelSubtreeTasks = new AtomicInteger();

  public LocalBulkDeleter(Path execRoot) {
    this(execRoot, POOL);
  }

  @VisibleForTesting
  LocalBulkDeleter(Path execRoot, ExecutorService pool) {
    this.execRoot = checkNotNull(execRoot);
    this.pool = checkNotNull(pool);
  }

  @VisibleForTesting
  int parallelSubtreeTasks() {
    return parallelSubtreeTasks.get();
  }

  @VisibleForTesting
  static boolean useParallelDelete(int childDirectoryCount) {
    return childDirectoryCount >= PARALLEL_CHILD_DIRECTORY_THRESHOLD;
  }

  @Override
  public void bulkDelete(Iterable<PathFragment> paths) throws IOException, InterruptedException {
    for (PathFragment fragment : paths) {
      Path path = execRoot.getRelative(fragment);
      try {
        // A file, a symlink, a missing path, and an empty directory are one remove(3).
        path.delete();
        continue;
      } catch (IOException ignored) {
        // ENOTEMPTY, or the parent directory is not writable.
      }
      Path parent = path.getParentDirectory();
      if (parent != null && parent.startsWith(execRoot)) {
        try {
          parent.setWritable(true);
        } catch (IOException ignored) {
          // The delete below reports the real failure.
        }
      }
      ImmutableList<Path> childDirectories = childDirectories(path);
      if (useParallelDelete(childDirectories.size())) {
        path.setWritable(true);
        deleteInParallel(childDirectories);
      }
      // Flat dirs and single-root chains never entered the parallel branch; deleteTree is the
      // same serial walk as before. After a parallel fan-out the remaining entries are few.
      path.deleteTree();
    }
  }

  private static ImmutableList<Path> childDirectories(Path path) {
    ImmutableList.Builder<Path> directories = ImmutableList.builder();
    try {
      if (!path.isDirectory(Symlinks.NOFOLLOW)) {
        return ImmutableList.of();
      }
      for (Dirent dirent : path.readdir(Symlinks.NOFOLLOW)) {
        if (dirent.getType() == Dirent.Type.DIRECTORY) {
          directories.add(path.getChild(dirent.getName()));
        }
      }
    } catch (IOException e) {
      return ImmutableList.of();
    }
    return directories.build();
  }

  // Waits for every task even if interrupted, so no delete outlives the action's prepare.
  private void deleteInParallel(ImmutableList<Path> directories) throws IOException {
    List<Future<?>> futures = new ArrayList<>(directories.size());
    for (Path directory : directories) {
      parallelSubtreeTasks.incrementAndGet();
      futures.add(
          pool.submit(
              () -> {
                directory.deleteTree();
                return null;
              }));
    }
    IOException error = null;
    for (Future<?> future : futures) {
      try {
        Uninterruptibles.getUninterruptibly(future);
      } catch (ExecutionException e) {
        Throwables.throwIfUnchecked(e.getCause());
        IOException cause =
            e.getCause() instanceof IOException io ? io : new IOException(e.getCause());
        if (error == null) {
          error = cause;
        } else {
          error.addSuppressed(cause);
        }
      }
    }
    if (error != null) {
      throw error;
    }
  }
}
