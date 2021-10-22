package s3sync

import (
	"context"
	"fmt"

	"github.com/regalias/s3-zip-deploy-go/pkg/bundle"
)

func (h *SyncHandler) CalculateFileListDiff(ctx context.Context, archiveManifest bundle.ArchiveManifest) ([]string, error) {

	objects, err := h.FetchObjectListFromTarget(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get object list: %v", err)
	}

	filesToDelete := make([]string, 0)

	for _, s3obj := range objects {
		// Check if the S3 object in target bucket is in the archive manifest
		if !archiveManifest[*s3obj.Key].IsZero() {
			// If S3 object exists in manifest, check last modified date to see if we need to update
			if !archiveManifest[*s3obj.Key].After(*s3obj.LastModified) {
				// File is not newer than s3 object, no need to upload
				// Remove it from the manifest
				delete(archiveManifest, *s3obj.Key)
			}
			// If not, leave file in manifest as it

		} else {
			// If S3 object isn't in current manifest, we need to delete this file
			filesToDelete = append(filesToDelete, *s3obj.Key)
		}
	}
	return filesToDelete, nil
}
