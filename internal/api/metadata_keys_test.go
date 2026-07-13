package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/stellwerk-labs/golib/hecho"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/authz"
	platformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	mockplatformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratorcp/mocks"
	mockplatformorchestratoriam "github.com/stellwerk-labs/platform-orchestrator-dp/internal/clients/platformorchestratoriam/mocks"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/model"
	mock_model "github.com/stellwerk-labs/platform-orchestrator-dp/internal/model/mocks"
)

func TestServer_CreateMetadataKey(t *testing.T) {
	const (
		orgId       = "org-id"
		metadataKey = "Metadata-Key"
	)
	errUnexpected := fmt.Errorf("unexpected error")

	tests := []struct {
		name                      string
		request                   CreateMetadataKeyRequestObject
		getOrganizationBehavior   func(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
		createMetadataKeyBehavior func(*mock_model.MockDatabaser)
		want                      CreateMetadataKeyResponseObject
		wantErr                   bool
	}{
		{
			name: "valid request",
			request: CreateMetadataKeyRequestObject{
				OrgId: orgId,
				Body: &CreateMetadataKeyJSONRequestBody{
					Name: metadataKey,
				},
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusOK,
					},
				}, nil)
			},
			createMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().CreateMetadataKey(gomock.Any(), nil, orgId, gomock.Any()).Return(&model.MetadataKey{
					Name: metadataKey,
				}, nil)
			},
			want: CreateMetadataKey201JSONResponse{
				Name: metadataKey,
			},
		},
		{
			name: "organization not found",
			request: CreateMetadataKeyRequestObject{
				OrgId: orgId,
				Body: &CreateMetadataKeyJSONRequestBody{
					Name: metadataKey,
				},
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusNotFound,
					},
				}, nil)
			},
			want: CreateMetadataKey404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
				Message: "organization org-id not found",
			})},
		},
		{
			name: "organization not found",
			request: CreateMetadataKeyRequestObject{
				OrgId: orgId,
				Body: &CreateMetadataKeyJSONRequestBody{
					Name: metadataKey,
				},
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusInternalServerError,
					},
				}, nil)
			},
			wantErr: true,
		},
		{
			name: "metadata key already exists",
			request: CreateMetadataKeyRequestObject{
				OrgId: orgId,
				Body: &CreateMetadataKeyJSONRequestBody{
					Name: metadataKey,
				},
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusOK,
					},
				}, nil)
			},
			createMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().CreateMetadataKey(gomock.Any(), nil, orgId, gomock.Any()).Return(nil, model.NewErrConflict("metadata_keys with name Metadata-Key already exists"))
			},
			want: CreateMetadataKey409JSONResponse{
				N409ConflictJSONResponse{
					Error:   "HTTP-409",
					Message: "metadata_keys with name Metadata-Key already exists",
				},
			},
		},
		{
			name: "creation of metadata key failed",
			request: CreateMetadataKeyRequestObject{
				OrgId: orgId,
				Body: &CreateMetadataKeyJSONRequestBody{
					Name: metadataKey,
				},
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusOK,
					},
				}, nil)
			},
			createMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().CreateMetadataKey(gomock.Any(), nil, orgId, gomock.Any()).Return(nil, errUnexpected)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()
			db := s.Database.(*mock_model.MockDatabaser)
			cpClinet := s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface)

			assert := assert.New(t)

			if tt.getOrganizationBehavior != nil {
				tt.getOrganizationBehavior(cpClinet)
			}

			if tt.createMetadataKeyBehavior != nil {
				tt.createMetadataKeyBehavior(db)
			}

			userId := userid.NewHumanUserId()
			mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
			mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
				UserId: userId,
				Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanManageOrgCheck(orgId)},
			}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
			}, nil)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
			got, err := s.CreateMetadataKey(ctx, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(got)
			} else {
				require.NoError(t, err)
				assert.NotNil(got)
				assert.Equal(tt.want, got)
			}
		})
	}
}

func TestServer_GetMetadataKey(t *testing.T) {
	const (
		orgId       = "org-id"
		metadataKey = "Metadata-Key"
	)

	errUnexpected := fmt.Errorf("unexpected error")

	tests := []struct {
		name                   string
		request                GetMetadataKeyRequestObject
		getMetadataKeyBehavior func(*mock_model.MockDatabaser)
		want                   GetMetadataKeyResponseObject
		wantErr                bool
	}{
		{
			name: "valid request",
			request: GetMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(&model.MetadataKey{
					Name: metadataKey,
				}, nil)
			},
			want: GetMetadataKey200JSONResponse{
				Name: metadataKey,
			},
		},
		{
			name: "metadata key not found",
			request: GetMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(nil, model.NewErrNotFound("metadata key not found"))
			},
			want: GetMetadataKey404JSONResponse{
				N404NotFoundJSONResponse{
					Error:   "HTTP-404",
					Message: "metadata key not found",
				},
			},
		},
		{
			name: "get metadata key failed",
			request: GetMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(nil, errUnexpected)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()
			db := s.Database.(*mock_model.MockDatabaser)

			assert := assert.New(t)

			if tt.getMetadataKeyBehavior != nil {
				tt.getMetadataKeyBehavior(db)
			}

			userId := userid.NewHumanUserId()
			mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
			mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
				UserId: userId,
				Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck(orgId)},
			}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
			}, nil)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
			got, err := s.GetMetadataKey(ctx, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(got)
			} else {
				require.NoError(t, err)
				assert.NotNil(got)
				assert.Equal(tt.want, got)
			}
		})
	}
}

func TestServer_ListMetadataKeys(t *testing.T) {
	const (
		orgId        = "org-id"
		metadataKey1 = "Metadata-Key-1"
		metadataKey2 = "Metadata-Key-2"
	)

	errUnexpected := fmt.Errorf("unexpected error")

	tests := []struct {
		name                    string
		request                 ListMetadataKeysRequestObject
		getOrganizationBehavior func(*mockplatformorchestratorcp.MockClientWithResponsesInterface)
		listMetadaKeysBehavior  func(*mock_model.MockDatabaser)
		want                    ListMetadataKeysResponseObject
		wantErr                 bool
	}{
		{
			name: "valid request",
			request: ListMetadataKeysRequestObject{
				OrgId: orgId,
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusOK,
					},
				}, nil)
			},
			listMetadaKeysBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().ListMetadataKeys(gomock.Any(), nil, orgId, "", defaultPaginationSize).Return([]*model.MetadataKey{
					{
						Name: metadataKey1,
					},
					{
						Name: metadataKey2,
					},
				}, "", nil)
			},
			want: ListMetadataKeys200JSONResponse(
				MetadataKeyPage{
					Items: []MetadataKey{
						{
							Name: metadataKey1,
						},
						{
							Name: metadataKey2,
						},
					},
				},
			),
		},
		{
			name: "organization not found",
			request: ListMetadataKeysRequestObject{
				OrgId: orgId,
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusNotFound,
					},
				}, nil)
			},
			want: ListMetadataKeys404JSONResponse{Generate404FromModelErr(model.ErrNotFound{
				Message: "organization org-id not found",
			})},
		},
		{
			name: "list metadata keys failed",
			request: ListMetadataKeysRequestObject{
				OrgId: orgId,
			},
			getOrganizationBehavior: func(cpClient *mockplatformorchestratorcp.MockClientWithResponsesInterface) {
				cpClient.EXPECT().GetInternalOrganizationWithResponse(gomock.Any(), orgId).Return(&platformorchestratorcp.GetInternalOrganizationResponse{
					HTTPResponse: &http.Response{
						StatusCode: http.StatusOK,
					},
				}, nil)
			},
			listMetadaKeysBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().ListMetadataKeys(gomock.Any(), nil, orgId, "", defaultPaginationSize).Return(nil, "", errUnexpected)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()
			db := s.Database.(*mock_model.MockDatabaser)

			assert := assert.New(t)

			if tt.getOrganizationBehavior != nil {
				tt.getOrganizationBehavior(s.ControlPlaneClient.(*mockplatformorchestratorcp.MockClientWithResponsesInterface))
			}

			if tt.listMetadaKeysBehavior != nil {
				tt.listMetadaKeysBehavior(db)
			}
			userId := userid.NewHumanUserId()
			mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
			mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
				UserId: userId,
				Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanReadOrgCheck(orgId)},
			}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
			}, nil)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
			got, err := s.ListMetadataKeys(ctx, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(got)
			} else {
				require.NoError(t, err)
				assert.NotNil(got)
				assert.Equal(tt.want, got)
			}
		})
	}
}

func TestServer_UpdateMetadataKey(t *testing.T) {
	const (
		orgId       = "org-id"
		metadataKey = "Metadata-Key"
	)
	var (
		metadataKeyDescription = "Description"
		metadataKeyType        = "string"
		metadataKeyFormat      = "date-time"
		metadataKeyPattern     = "^[0-9]{4}-[0-9]{2}-[0-9]{2}$"
	)

	newDescription := "New description"
	newType := "integer"
	newFormat := "int32"
	newPattern := "^[0-9]{4}$"

	errUnexpected := fmt.Errorf("unexpected error")

	tests := []struct {
		name                      string
		request                   UpdateMetadataKeyRequestObject
		getMetadataKeyBehavior    func(*mock_model.MockDatabaser)
		updateMetadataKeyBehavior func(*mock_model.MockDatabaser)
		want                      UpdateMetadataKeyResponseObject
		wantErr                   bool
	}{
		{
			name: "update metadata key description successfully",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(&model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}, nil)
			},
			updateMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().UpdateMetadataKey(gomock.Any(), gomock.Any(), orgId, &model.MetadataKey{
					Name:        metadataKey,
					Description: &newDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}).Return(nil)
			},
			want: UpdateMetadataKey200JSONResponse{
				Name:        metadataKey,
				Description: &newDescription,
				Schema: MetadataKeySchema{
					Type:    MetadataKeySchemaType(metadataKeyType),
					Format:  &metadataKeyFormat,
					Pattern: &metadataKeyPattern,
				},
			},
		},
		{
			name: "update metadata key description and type successfully",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
					Schema: &UpdateMetadataKeySchema{
						Type: (*UpdateMetadataKeySchemaType)(&newType),
					},
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(&model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}, nil)
			},
			updateMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().UpdateMetadataKey(gomock.Any(), gomock.Any(), orgId, &model.MetadataKey{
					Name:        metadataKey,
					Description: &newDescription,
					Schema: model.MetadataKeySchema{
						Type:    newType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}).Return(nil)
			},
			want: UpdateMetadataKey200JSONResponse{
				Name:        metadataKey,
				Description: &newDescription,
				Schema: MetadataKeySchema{
					Type:    MetadataKeySchemaType(newType),
					Format:  &metadataKeyFormat,
					Pattern: &metadataKeyPattern,
				},
			},
		},
		{
			name: "update metadata key format and pattern successfully",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Schema: &UpdateMetadataKeySchema{
						Format:  &newFormat,
						Pattern: &newPattern,
					},
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(&model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}, nil)
			},
			updateMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().UpdateMetadataKey(gomock.Any(), gomock.Any(), orgId, &model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &newFormat,
						Pattern: &newPattern,
					},
				}).Return(nil)
			},
			want: UpdateMetadataKey200JSONResponse{
				Name:        metadataKey,
				Description: &metadataKeyDescription,
				Schema: MetadataKeySchema{
					Type:    MetadataKeySchemaType(metadataKeyType),
					Format:  &newFormat,
					Pattern: &newPattern,
				},
			},
		},
		{
			name: "get metadata key not found",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(nil, model.NewErrNotFound("metadata key not found"))
			},
			want: UpdateMetadataKey404JSONResponse{
				N404NotFoundJSONResponse{
					Error:   "HTTP-404",
					Message: "metadata key not found",
				},
			},
		},
		{
			name: "get metadata key failed",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(nil, errUnexpected)
			},
			wantErr: true,
		},
		{
			name: "update metadata key not found",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(&model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}, nil)
			},
			updateMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().UpdateMetadataKey(gomock.Any(), gomock.Any(), orgId, &model.MetadataKey{
					Name:        metadataKey,
					Description: &newDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}).Return(model.NewErrNotFound("metadata key not found"))
			},
			want: UpdateMetadataKey404JSONResponse{
				N404NotFoundJSONResponse{
					Error:   "HTTP-404",
					Message: "metadata key not found",
				},
			},
		},
		{
			name: "update metadata key failed",
			request: UpdateMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
				Body: &UpdateMetadataKeyJSONRequestBody{
					Description: &newDescription,
				},
			},
			getMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().GetMetadataKey(gomock.Any(), gomock.Any(), orgId, metadataKey).Return(&model.MetadataKey{
					Name:        metadataKey,
					Description: &metadataKeyDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}, nil)
			},
			updateMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().UpdateMetadataKey(gomock.Any(), gomock.Any(), orgId, &model.MetadataKey{
					Name:        metadataKey,
					Description: &newDescription,
					Schema: model.MetadataKeySchema{
						Type:    metadataKeyType,
						Format:  &metadataKeyFormat,
						Pattern: &metadataKeyPattern,
					},
				}).Return(errUnexpected)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()
			db := s.Database.(*mock_model.MockDatabaser)

			assert := assert.New(t)

			if tt.getMetadataKeyBehavior != nil {
				tt.getMetadataKeyBehavior(db)
			}
			if tt.updateMetadataKeyBehavior != nil {
				tt.updateMetadataKeyBehavior(db)
			}

			userId := userid.NewHumanUserId()
			mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
			mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
				UserId: userId,
				Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanManageOrgCheck(orgId)},
			}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
			}, nil)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
			got, err := s.UpdateMetadataKey(ctx, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(got)
			} else {
				require.NoError(t, err)
				assert.NotNil(got)
				assert.Equal(tt.want, got)
			}
		})
	}
}

func TestServer_DeleteMetadataKey(t *testing.T) {
	const (
		orgId       = "org-id"
		metadataKey = "Metadata-Key"
	)

	errUnexpected := fmt.Errorf("unexpected error")

	tests := []struct {
		name                      string
		request                   DeleteMetadataKeyRequestObject
		deleteMetadataKeyBehavior func(*mock_model.MockDatabaser)
		want                      DeleteMetadataKeyResponseObject
		wantErr                   bool
	}{
		{
			name: "valid request",
			request: DeleteMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			deleteMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().DeleteMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(nil)
			},
			want: DeleteMetadataKey204Response{},
		},
		{
			name: "metadata key not found",
			request: DeleteMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			deleteMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().DeleteMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(model.NewErrNotFound("metadata key not found"))
			},
			want: DeleteMetadataKey404JSONResponse{
				N404NotFoundJSONResponse{
					Error:   "HTTP-404",
					Message: "metadata key not found",
				},
			},
		},
		{
			name: "delete metadata key failed",
			request: DeleteMetadataKeyRequestObject{
				OrgId:           orgId,
				MetadataKeyName: metadataKey,
			},
			deleteMetadataKeyBehavior: func(db *mock_model.MockDatabaser) {
				db.EXPECT().DeleteMetadataKey(gomock.Any(), nil, orgId, metadataKey).Return(errUnexpected)
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, s, fin := MockServer(t)
			defer fin()
			db := s.Database.(*mock_model.MockDatabaser)

			assert := assert.New(t)

			if tt.deleteMetadataKeyBehavior != nil {
				tt.deleteMetadataKeyBehavior(db)
			}

			userId := userid.NewHumanUserId()
			mockIamClient := s.IamClient.(*mockplatformorchestratoriam.MockClientWithResponsesInterface)
			mockIamClient.EXPECT().InternalAuthorizeWithResponse(gomock.Any(), platformorchestratoriam.InternalAuthorizeBody{
				UserId: userId,
				Checks: []platformorchestratoriam.ResourcePermissionCheck{authz.CanManageOrgCheck(orgId)},
			}).Return(&platformorchestratoriam.InternalAuthorizeResponse{
				HTTPResponse: &http.Response{StatusCode: http.StatusNoContent},
			}, nil)

			ctx := context.WithValue(t.Context(), hecho.ContextKeyUserID, userId.String())
			got, err := s.DeleteMetadataKey(ctx, tt.request)
			if tt.wantErr {
				require.Error(t, err)
				assert.Nil(got)
			} else {
				require.NoError(t, err)
				assert.NotNil(got)
				assert.Equal(tt.want, got)
			}
		})
	}
}
