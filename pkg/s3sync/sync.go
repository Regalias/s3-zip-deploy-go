package s3sync

import (
	"context"
	"fmt"
	"os"

	"github.com/regalias/s3-sync-zip-go/pkg/bundle"
)

func (h *SyncHandler) SyncBucket(ctx context.Context) error {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Create a tempdir
	tmp, err := os.MkdirTemp("", "s3sync")
	if err != nil {
		return fmt.Errorf("failed to create temp dir: %v", err)
	}

	// Change working dir to temp directory
	if err := os.Chdir(tmp); err != nil {
		return fmt.Errorf("failed to change working dir to temp dir: %v", err)
	}

	// Fetch the bundle from s3
	if err := h.DownloadSourceBundle(ctx, "bundle.zip"); err != nil {
		return fmt.Errorf("failed to fetch source bundle: %v", err)
	}

	// Extract the zip to tempdir
	manifest, err := bundle.DecompressBundle("bundle.zip", "./")
	if err != nil {
		return fmt.Errorf("failed to decompress bundle :%v", err)
	}

	// Find manifest differences
	toDelete, err := h.CalculateFileListDiff(ctx, manifest)
	if err != nil {
		return fmt.Errorf("could not calculate file diff: %v", err)
	}

	// Upload new files
	toUpload := make([]string, len(manifest))
	i := 0
	for k := range manifest {
		toUpload[i] = k
		i++
	}

	// UploadFileListThreaded
	// UploadFileList
	if err := h.UploadFileListThreaded(ctx, toUpload); err != nil {
		return fmt.Errorf("failed to upload files: %v", err)
	}

	// Clean up old files
	if err := h.DeleteObjects(ctx, toDelete); err != nil {
		return fmt.Errorf("failed to delete objects: %v", err)
	}

	return nil
}
