package integrationtests

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/genclient"
)

func TestMetadataKeys(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	dpClient := MustDataPlaneClient(t)
	testOrg := MustCreateOrgId(t, MustInternalControlPlaneClient(t))

	t.Run("should not create metadata key with invalid name", func(t *testing.T) {
		createResp, err := dpClient.CreateMetadataKeyWithResponse(ctx, testOrg, serverclient.CreateMetadataKeyJSONRequestBody{
			Name: "invalidTestKey",
			Schema: serverclient.MetadataKeySchema{
				Type: serverclient.MetadataKeySchemaTypeString,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, createResp.StatusCode())
	})

	t.Run("should create metadata key", func(t *testing.T) {
		description := "Test description"
		format := "date-time"
		pattern := "$[1-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z"

		createResp, err := dpClient.CreateMetadataKeyWithResponse(ctx, testOrg, serverclient.CreateMetadataKeyJSONRequestBody{
			Name:        "Test-Key-1",
			Description: &description,
			Schema: serverclient.MetadataKeySchema{
				Type:    serverclient.MetadataKeySchemaTypeString,
				Format:  &format,
				Pattern: &pattern,
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, createResp.StatusCode())
		assert.NotNil(t, createResp.JSON201)
		assert.Equal(t, "Test-Key-1", createResp.JSON201.Name)
		assert.Equal(t, description, *createResp.JSON201.Description)
		assert.NotEmpty(t, createResp.JSON201.CreatedAt)
		assert.Equal(t, serverclient.MetadataKeySchemaTypeString, createResp.JSON201.Schema.Type)
		assert.Equal(t, format, *createResp.JSON201.Schema.Format)
		assert.Equal(t, pattern, *createResp.JSON201.Schema.Pattern)
	})

	t.Run("should return conflict when metadata key already exists", func(t *testing.T) {
		createResp, err := dpClient.CreateMetadataKeyWithResponse(ctx, testOrg, serverclient.CreateMetadataKeyJSONRequestBody{
			Name: "Test-Key-1",
			Schema: serverclient.MetadataKeySchema{
				Type: serverclient.MetadataKeySchemaTypeString,
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, createResp.StatusCode())
		require.NotNil(t, createResp.JSON409)
		assert.Equal(t, "HTTP-409", createResp.JSON409.Error)
		assert.Equal(t, "metadata_keys with name Test-Key-1 already exists", createResp.JSON409.Message)
	})

	t.Run("should get metadata key", func(t *testing.T) {
		getResp, err := dpClient.GetMetadataKeyWithResponse(ctx, testOrg, "Test-Key-1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, getResp.StatusCode())
		assert.NotNil(t, getResp.JSON200)
		assert.Equal(t, "Test-Key-1", getResp.JSON200.Name)
	})

	t.Run("should add another metadata key", func(t *testing.T) {
		createResp, err := dpClient.CreateMetadataKeyWithResponse(ctx, testOrg, serverclient.CreateMetadataKeyJSONRequestBody{
			Name: "Test-Key-2",
			Schema: serverclient.MetadataKeySchema{
				Type: serverclient.MetadataKeySchemaTypeString,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusCreated, createResp.StatusCode())
		assert.NotNil(t, createResp.JSON201)
		assert.Equal(t, "Test-Key-2", createResp.JSON201.Name)
		assert.Equal(t, serverclient.MetadataKeySchemaTypeString, createResp.JSON201.Schema.Type)
	})

	t.Run("should list metadata keys", func(t *testing.T) {
		listResp, err := dpClient.ListMetadataKeysWithResponse(ctx, testOrg, nil)
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, listResp.StatusCode())
		assert.NotNil(t, listResp.JSON200)
		assert.Len(t, listResp.JSON200.Items, 2)
	})

	t.Run("should update metadata key description", func(t *testing.T) {
		description := "Updated description"

		updateResp, err := dpClient.UpdateMetadataKeyWithResponse(ctx, testOrg, "Test-Key-1", serverclient.UpdateMetadataKeyJSONRequestBody{
			Description: &description,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, updateResp.StatusCode())
		assert.NotNil(t, updateResp.JSON200)
		assert.Equal(t, description, *updateResp.JSON200.Description)
	})

	t.Run("should update metadata key format", func(t *testing.T) {
		format := "date-time"

		updateResp, err := dpClient.UpdateMetadataKeyWithResponse(ctx, testOrg, "Test-Key-1", serverclient.UpdateMetadataKeyJSONRequestBody{
			Schema: &serverclient.UpdateMetadataKeySchema{
				Format: &format,
			},
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, updateResp.StatusCode())
		assert.NotNil(t, updateResp.JSON200)
		assert.Equal(t, format, *updateResp.JSON200.Schema.Format)
	})

	t.Run("should delete metadata key", func(t *testing.T) {
		deleteResp, err := dpClient.DeleteMetadataKeyWithResponse(ctx, testOrg, "Test-Key-1")
		require.NoError(t, err)
		assert.Equal(t, http.StatusNoContent, deleteResp.StatusCode())
	})

	t.Run("should add 50 metadata keys", func(t *testing.T) {
		for i := 3; i < 53; i++ {
			createResp, err := dpClient.CreateMetadataKeyWithResponse(ctx, testOrg, serverclient.CreateMetadataKeyJSONRequestBody{
				Name: fmt.Sprintf("Test-Key-%d", i),
				Schema: serverclient.MetadataKeySchema{
					Type: serverclient.MetadataKeySchemaTypeString,
				},
			})
			require.NoError(t, err)
			assert.Equal(t, http.StatusCreated, createResp.StatusCode())
			assert.NotNil(t, createResp.JSON201)
			assert.Equal(t, fmt.Sprintf("Test-Key-%d", i), createResp.JSON201.Name)
			assert.Equal(t, serverclient.MetadataKeySchemaTypeString, createResp.JSON201.Schema.Type)
		}
	})

	t.Run("should list metadata keys with pagination", func(t *testing.T) {
		perPage := 15

		listResp, err := dpClient.ListMetadataKeysWithResponse(ctx, testOrg, &serverclient.ListMetadataKeysParams{
			Page:    nil,
			PerPage: &perPage,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, listResp.StatusCode())
		assert.NotNil(t, listResp.JSON200)
		assert.Len(t, listResp.JSON200.Items, perPage)

		nextListResp, err := dpClient.ListMetadataKeysWithResponse(ctx, testOrg, &serverclient.ListMetadataKeysParams{
			Page:    listResp.JSON200.NextPageToken,
			PerPage: &perPage,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, nextListResp.StatusCode())
		assert.NotNil(t, nextListResp.JSON200)
		assert.Len(t, nextListResp.JSON200.Items, perPage)

		assert.NotEqual(t, listResp.JSON200.Items, nextListResp.JSON200.Items)
	})
}
