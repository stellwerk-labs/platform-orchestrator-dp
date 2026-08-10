package integrationtests

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"slices"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/google/uuid"
	platformorchestratorcp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient"
	"github.com/stellwerk-labs/platform-orchestrator-iam/shared/userid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/v2/genclient"
)

func TestDeployments(t *testing.T) {
	t.Parallel()
	client := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	internalDpClient := MustInternalDataPlaneClient(t)
	dbConn := MustDatabaseConn(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "my-app").Id
	runnerId := MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "my-env")

	t.Run("must authorize request - failure", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{}, func(ctx context.Context, req *http.Request) error {
			req.Header.Set("From", userid.NewHumanUserId().String())
			return nil
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusForbidden, res.StatusCode())
	})

	t.Run("list empty", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Empty(t, res.JSON200.Items)
			assert.Empty(t, res.JSON200.NextPageToken)
		}
	})

	t.Run("list last empty", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Empty(t, res.JSON200.Items)
			assert.Empty(t, res.JSON200.NextPageToken)
		}
	})

	t.Run("get not found", func(t *testing.T) {
		unknownDep := uuid.New()
		res, err := client.GetDeploymentWithResponse(t.Context(), orgId, unknownDep)
		if assert.NoError(t, err) && assert.Equal(t, http.StatusNotFound, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, "deployment '"+unknownDep.String()+"' not found", res.JSON404.Message)
		}
	})

	t.Run("delete no effect", func(t *testing.T) {
		res, err := internalDpClient.InternalDeleteDeploymentsWithResponse(t.Context(), orgId, &serverclient.InternalDeleteDeploymentsParams{ProjectId: ref.Ref("unknown"), EnvId: ref.Ref("unknown")})
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
		}
	})

	iamClient := MustIamClient(t)
	tut := MustGenerateTestUserToken(t)
	userId := MustRegisterUser(t, iamClient, tut)
	internalIamClient := MustInternalIamClient(t)
	MustCreateMembershipInOrg(t, internalIamClient, iamClient, orgId, "Viewer", "", userId)
	viewerUserClient := MustDataPlaneClientWithUserId(t, userId.String())

	t.Run("viewer cannot create deployment", func(t *testing.T) {
		projectId, envId := env.ProjectId, env.Id
		res, err := viewerUserClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: projectId,
			EnvId:     envId,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
			EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusForbidden, res.StatusCode(), string(res.Body)) {
			assert.Contains(t, string(res.Body), fmt.Sprintf(`"permission":"write","resource":"env:%s"`, env.Uuid.String()))
		}
	})

	// assign env write permission to the user
	MustCreateMembershipInOrg(t, internalIamClient, iamClient, orgId, "Admin", "env:"+env.Uuid.String(), userId)

	var dep serverclient.Deployment
	t.Run("create a deployment", func(t *testing.T) {
		projectId, envId := env.ProjectId, env.Id
		require.EventuallyWithT(t, func(collect *assert.CollectT) {
			res, err := viewerUserClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
				ProjectId: projectId,
				EnvId:     envId,
				Mode:      serverclient.DeploymentCreateBodyModeDeploy,
				Manifest: &serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {},
					},
				},
				EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
			})
			if assert.NoError(t, err) && assert.Equal(collect, http.StatusCreated, res.StatusCode(), string(res.Body)) {
				assert.Equal(t, orgId, res.JSON201.OrgId)
				assert.Equal(t, projectId, res.JSON201.ProjectId)
				assert.Equal(t, envId, res.JSON201.EnvId)
				assert.NotEmpty(t, res.JSON201.Id)
				assert.Equal(t, "deploy", res.JSON201.Mode)
				assert.False(t, res.JSON201.PlanOnly)
				assert.NotEmpty(t, res.JSON201.CreatedAt)
				assert.NotEmpty(t, res.JSON201.CreatedBy)
				assert.Empty(t, res.JSON201.CompletedAt)
				assert.Equal(t, "executing", res.JSON201.Status)
				assert.NotEmpty(t, res.JSON201.StatusMessage)
				assert.Equal(t, runnerId, res.JSON201.RunnerId)
				assert.Equal(t, serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {},
					},
				}, res.JSON201.Manifest)
				dep = *res.JSON201
			}
		}, 30*time.Second, 3*time.Second, "deployment creation failed")
	})

	t.Run("deployment has a valid json graph and tofu", func(t *testing.T) {
		var rawGraph json.RawMessage
		var rawTofu []byte
		if err := dbConn.QueryRowContext(t.Context(), `SELECT d.graph, d.tofu FROM deployments d WHERE d.id = $1`, dep.Id).Scan(&rawGraph, &rawTofu); assert.NoError(t, err) {
			var out map[string]interface{}
			require.NoError(t, json.Unmarshal(rawGraph, &out))
			assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "`+env.Uuid.String()+`"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
  }
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "sample" {
  value       = {}
  description = "The output variables for workload 'sample'"
  sensitive   = true
}
`, string(rawTofu))
		}
	})

	t.Run("get deployment returns the same thing", func(t *testing.T) {
		res, err := client.GetDeploymentWithResponse(t.Context(), orgId, dep.Id)
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, dep, *res.JSON200)
		}
	})

	var depSummary serverclient.DeploymentSummary
	t.Run("new deployment is in the list", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			depSummary = res.JSON200.Items[0]
			assert.Equal(t, orgId, depSummary.OrgId)
			assert.Equal(t, dep.ProjectId, depSummary.ProjectId)
			assert.Equal(t, dep.EnvId, depSummary.EnvId)
			assert.Equal(t, dep.Id, depSummary.Id)
			assert.Equal(t, dep.CreatedAt, depSummary.CreatedAt)
			assert.Equal(t, dep.CreatedBy, depSummary.CreatedBy)
			assert.Empty(t, depSummary.CompletedAt)
			assert.Equal(t, "executing", depSummary.Status)
			assert.Equal(t, dep.StatusMessage, depSummary.StatusMessage)
		}
	})

	t.Run("new deployment is in the list when filtered by env ID", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			assert.Equal(t, depSummary, res.JSON200.Items[0])
		}
	})

	t.Run("new deployment is not in the list when filtered out", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref("unknown")})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Empty(t, res.JSON200.Items)
		}
	})

	t.Run("new deployment is in the last-list", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			assert.Equal(t, depSummary, res.JSON200.Items[0])
		}
	})

	t.Run("new deployment is in the last-list when filtered", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			assert.Equal(t, depSummary, res.JSON200.Items[0])
		}
	})

	t.Run("new deployment is not in the last-list when filtered out", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref("unknown")})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Empty(t, res.JSON200.Items)
		}
	})

	t.Run("deployment succeeds", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, client, orgId, dep.Id)
		assert.Equal(t, "succeeded", r.Status, string(r.StatusMessage))
	})

	t.Run("cannot delete when not cleaned up", func(t *testing.T) {
		res, err := internalDpClient.InternalDeleteDeploymentsWithResponse(t.Context(), orgId, &serverclient.InternalDeleteDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, "cannot delete deployment environment without a successful destroy", res.JSON409.Message)
		}
	})

	t.Run("cannot set plan only to false for plan_only mode", func(t *testing.T) {
		res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: dep.ProjectId,
			EnvId:     dep.EnvId,
			Mode:      serverclient.DeploymentCreateBodyModePlanOnly,
			PlanOnly:  ref.Ref(false),
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
		assert.Equal(t, "plan_only cannot be false if mode is plan_only", res.JSON400.Message)
	})

	var newDep serverclient.Deployment
	t.Run("can create another deployment when completed", func(t *testing.T) {
		res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: dep.ProjectId,
			EnvId:     dep.EnvId,
			Mode:      serverclient.DeploymentCreateBodyModePlanOnly,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
			assert.NotEqual(t, dep.Id, res.JSON201.Id)
			newDep = *res.JSON201
		}
	})

	t.Run("both deployments are in the list", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 2) {
			assert.Equal(t, newDep.Id, res.JSON200.Items[0].Id)
			assert.Equal(t, dep.Id, res.JSON200.Items[1].Id)
		}
	})

	t.Run("only new deployment is in the last-list", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			assert.Equal(t, newDep.Id, res.JSON200.Items[0].Id)
		}
	})

	t.Run("ignore plans in list last", func(t *testing.T) {
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{StateChangeOnly: ref.Ref(true)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 1) {
			assert.Empty(t, res.JSON200.NextPageToken)
			assert.Equal(t, dep.Id, res.JSON200.Items[0].Id)
		}
	})

	t.Run("deployment succeeds", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, client, orgId, newDep.Id)
		assert.Equal(t, "succeeded", r.Status, string(r.StatusMessage))
	})

	t.Run("create a deployment with deprecated variables", func(t *testing.T) {
		projectId, envId := env.ProjectId, env.Id
		{
			res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
				ProjectId: projectId,
				EnvId:     envId,
				Mode:      serverclient.DeploymentCreateBodyModeDeploy,
				Manifest: &serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {
							Variables: map[string]string{
								"foo": "bar",
							},
						},
					},
				},
				EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
			})
			if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
				assert.Equal(t, orgId, res.JSON201.OrgId)
				assert.Equal(t, projectId, res.JSON201.ProjectId)
				assert.Equal(t, envId, res.JSON201.EnvId)
				assert.NotEmpty(t, res.JSON201.Id)
				assert.NotEmpty(t, res.JSON201.CreatedAt)
				assert.NotEmpty(t, res.JSON201.CreatedBy)
				assert.Empty(t, res.JSON201.CompletedAt)
				assert.Equal(t, "executing", res.JSON201.Status)
				assert.NotEmpty(t, res.JSON201.StatusMessage)
				assert.Equal(t, runnerId, res.JSON201.RunnerId)
				assert.Equal(t, serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {
							Outputs: map[string]string{
								"foo": "bar",
							},
						},
					},
				}, res.JSON201.Manifest)
			}
			dep = *res.JSON201
		}
	})

	t.Run("deployment succeeds", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, client, orgId, dep.Id)
		assert.Equal(t, "succeeded", r.Status, string(r.StatusMessage))
	})

	t.Run("can create 10 more deployments", func(t *testing.T) {
		for i := 0; i < 10; i++ {
			envRes := MustCreateEnv(t, cpClient, orgId, envType, appId, fmt.Sprintf("env-%d", i))

			res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
				ProjectId: dep.ProjectId,
				EnvId:     envRes.Id,
				Mode:      serverclient.DeploymentCreateBodyModeDeploy,
				Manifest: &serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {},
					},
				},
			})
			if assert.NoError(t, err) {
				assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
			}
		}
	})

	t.Run("paginate list correctly", func(t *testing.T) {
		seenEnvs := make([]string, 0)
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{PerPage: ref.Ref(6)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 6) {
			for _, item := range res.JSON200.Items {
				seenEnvs = append(seenEnvs, item.EnvId)
			}
			if assert.NotEmpty(t, res.JSON200.NextPageToken) {
				res, err = client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{Page: res.JSON200.NextPageToken, PerPage: ref.Ref(7)})
				if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 7) {
					assert.Empty(t, res.JSON200.NextPageToken)
					for _, item := range res.JSON200.Items {
						seenEnvs = append(seenEnvs, item.EnvId)
					}
				}
			}
		}
		slices.Sort(seenEnvs)
		assert.Equal(t, []string{"env-0", "env-1", "env-2", "env-3", "env-4", "env-5", "env-6", "env-7", "env-8", "env-9", "my-env", "my-env", "my-env"}, seenEnvs)
	})

	t.Run("paginate last list correctly", func(t *testing.T) {
		seenEnvs := make([]string, 0)
		res, err := client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{PerPage: ref.Ref(6)})

		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 6) {
			for _, item := range res.JSON200.Items {
				seenEnvs = append(seenEnvs, item.EnvId)
			}

			assert.NotEmpty(t, res.JSON200.NextPageToken)
			res, err = client.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{Page: res.JSON200.NextPageToken, PerPage: ref.Ref(6)})
			if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) && assert.Len(t, res.JSON200.Items, 5) {
				assert.Empty(t, res.JSON200.NextPageToken)
				for _, item := range res.JSON200.Items {
					seenEnvs = append(seenEnvs, item.EnvId)
				}
			}
		}

		assert.Equal(t, []string{"env-0", "env-1", "env-2", "env-3", "env-4", "env-5", "env-6", "env-7", "env-8", "env-9", "my-env"}, seenEnvs)
	})

	t.Run("wait for all deployments to succeed", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			for _, item := range res.JSON200.Items {
				r := MustWaitForDeploymentComplete(t, client, orgId, item.Id)
				assert.Equal(t, "succeeded", r.Status, string(r.StatusMessage))
			}
		}
	})

	t.Run("cannot ask for any bundle without deployment token", func(t *testing.T) {
		{
			projectId, envId := env.ProjectId, env.Id
			res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
				ProjectId: projectId,
				EnvId:     envId,
				Mode:      serverclient.DeploymentCreateBodyModeDeploy,
				Manifest: &serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"sample": {},
					},
				},
				EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
			})
			if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
				dep = *res.JSON201
			}
		}

		res, err := client.GetDeploymentBundleWithResponse(t.Context(), orgId, dep.Id, &serverclient.GetDeploymentBundleParams{})
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
			assert.Contains(t, string(res.Body), `{"error":"HTTP-400","message":"parameter \"X-Deployment-Token\" in header has an error: empty value is not allowed"}`)
		}
	})

	t.Run("cannot ask for any bundle with a wrong deployment token", func(t *testing.T) {
		res, err := client.GetDeploymentBundleWithResponse(t.Context(), orgId, dep.Id, &serverclient.GetDeploymentBundleParams{XDeploymentToken: "t0k3n"})
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
			assert.Contains(t, string(res.Body), "the runner token is not valid")
		}
	})

	t.Run("can ask for any bundle with the proper deployment token", func(t *testing.T) {
		res, err := client.GetDeploymentBundleWithResponse(t.Context(), orgId, dep.Id, &serverclient.GetDeploymentBundleParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())})
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
			assert.Equal(t, map[string]string{
				"main.tf": fmt.Sprintf(`terraform {
  backend "kubernetes" {
    secret_suffix     = "%s"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
  }
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "sample" {
  value       = {}
  description = "The output variables for workload 'sample'"
  sensitive   = true
}
`, env.Uuid.String()),
			}, checkBundleContent(res.Body, t))
		}
	})

	t.Run("wait for success", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, client, orgId, dep.Id)
		assert.Equal(t, "succeeded", r.Status, string(r.StatusMessage))
	})

	t.Run("can grab the outputs", func(t *testing.T) {
		res, err := client.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			x, err := base64.StdEncoding.DecodeString(res.JSON200.Raw)
			require.NoError(t, err)
			assert.Contains(t, string(x), "age-encryption.org/v1")
		}
	})

	t.Run("cannot update the same deployment again", func(t *testing.T) {
		res, err := client.UpdateDeploymentResultsWithResponse(t.Context(), orgId, dep.Id, &serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())}, serverclient.DeploymentResultsUpdateBody{
			Status: serverclient.Success,
			TfResourceCounts: serverclient.DeploymentTFResourceCounts{
				NumResources:        12,
				NumResourcesAdded:   10,
				NumResourcesChanged: 2,
				NumResourcesRemoved: 2,
			},
		})
		if assert.NoError(t, err) {
			assert.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body))
		}
	})

	t.Run("the cleaner routine should remove the deploment outputs when they are staled", func(t *testing.T) {
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			var status string
			var outputs []byte
			if assert.NoError(c, dbConn.QueryRowContext(t.Context(), `SELECT status, outputs FROM deployments WHERE id = $1`, dep.Id).Scan(&status, &outputs)) {
				assert.NotEmpty(c, status)
				assert.Empty(c, outputs)
			}
		}, 30*time.Second, 2*time.Second, "outputs not deleted after 30s")
	})

	t.Run("cannot grab the outputs after removal", func(t *testing.T) {
		res, err := client.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
		if assert.NoError(t, err) && assert.Equal(t, http.StatusNotFound, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, "deployment outputs are no longer available", res.JSON404.Message)
		}
	})

	t.Run("cannot delete if last deployment is not a destroy", func(t *testing.T) {
		res, err := internalDpClient.InternalDeleteDeploymentsWithResponse(t.Context(), orgId, &serverclient.InternalDeleteDeploymentsParams{ProjectId: ref.Ref(dep.ProjectId), EnvId: ref.Ref(dep.EnvId)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, "cannot delete deployment environment without a successful destroy", res.JSON409.Message)
		}
	})

	t.Run("can destroy environment", func(t *testing.T) {
		res, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, env.ProjectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, res.StatusCode(), string(res.Body))
		require.EventuallyWithTf(t, func(collect *assert.CollectT) {
			res, err := cpClient.GetEnvironmentWithResponse(t.Context(), dep.OrgId, dep.ProjectId, dep.EnvId)
			require.NoError(collect, err)
			require.Equal(collect, http.StatusNotFound, res.StatusCode(), string(res.Body))
		}, time.Minute, time.Second, "failed to cleanup environment %s/%s/%s", dep.OrgId, env.ProjectId, env.Id)
	})
}

func TestDeploymentFiltering(t *testing.T) {
	t.Parallel()
	client := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "filter-app").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "filter-env")

	createDeploymentWithStatus := func(mode serverclient.DeploymentCreateBodyMode, resultStatus serverclient.DeploymentResultsUpdateBodyStatus) serverclient.Deployment {
		createBody := serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      mode,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
		}

		res, err := client.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, createBody)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))

		updateRes, err := client.UpdateDeploymentResultsWithResponse(t.Context(), orgId, res.JSON201.Id,
			&serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, res.JSON201.Id.String())},
			serverclient.DeploymentResultsUpdateBody{Status: resultStatus})
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, updateRes.StatusCode(), string(updateRes.Body))

		return *res.JSON201
	}

	// Create 3 deployments with different modes and statuses:
	dep1 := createDeploymentWithStatus(serverclient.DeploymentCreateBodyModeDeploy, serverclient.Success)
	dep2 := createDeploymentWithStatus(serverclient.DeploymentCreateBodyModePlanOnly, serverclient.Success)
	dep3 := createDeploymentWithStatus(serverclient.DeploymentCreateBodyModeDeploy, serverclient.Failure)

	t.Run("filter: unset should return all", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 3, "expected 3 deployments when filters are empty")
	})

	t.Run("filter: empty list not accepted", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByMode:    &[]string{},
			ByStatus:  &[]string{},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
	})

	t.Run("filter by mode: deploy only returns 2 deployments", func(t *testing.T) {
		// Filter by deploy mode (no status filter) - should return dep1 and dep3
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByMode:    &[]string{string(serverclient.ListDeploymentsParamsByModeDeploy)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 2, "expected 2 deployments with deploy mode")
		for _, item := range res.JSON200.Items {
			assert.Equal(t, "deploy", item.Mode)
			assert.False(t, item.PlanOnly)
		}
	})

	t.Run("filter by single status returns only deployments with that status", func(t *testing.T) {
		// Filter by succeeded status - should return dep1 and dep2
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByStatus:  &[]string{string(serverclient.Succeeded)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 2, "expected 2 deployments with succeeded status")
		for _, item := range res.JSON200.Items {
			assert.Equal(t, "succeeded", item.Status)
		}
	})

	t.Run("filter by multiple statuses returns deployments with any of those statuses", func(t *testing.T) {
		// Filter by succeeded and failed statuses - should return all 3 deployments
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByStatus:  &[]string{string(serverclient.Succeeded), string(serverclient.Failed)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 3, "expected all 3 deployments")

		// Verify all returned deployments have either succeeded or failed status
		for _, item := range res.JSON200.Items {
			assert.True(t, item.Status == "succeeded" || item.Status == "failed",
				"expected status to be succeeded or failed, got %s", item.Status)
		}
	})

	t.Run("filter by mode and status combined", func(t *testing.T) {
		// Filter by deploy mode and succeeded status - should return only dep1
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByMode:    &[]string{string(serverclient.ListDeploymentsParamsByModeDeploy)},
			ByStatus:  &[]string{string(serverclient.Succeeded)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 1, "expected 1 deployment with deploy mode and succeeded status")
		assert.Equal(t, dep1.Id, res.JSON200.Items[0].Id)
		assert.Equal(t, "deploy", res.JSON200.Items[0].Mode)
		assert.False(t, res.JSON200.Items[0].PlanOnly)
		assert.Equal(t, "succeeded", res.JSON200.Items[0].Status)
	})

	t.Run("filter by failed status returns only failed deployment", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByStatus:  &[]string{string(serverclient.Failed)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 1, "expected 1 deployment with failed status")
		assert.Equal(t, dep3.Id, res.JSON200.Items[0].Id)
		assert.Equal(t, "deploy", res.JSON200.Items[0].Mode)
		assert.False(t, res.JSON200.Items[0].PlanOnly)
		assert.Equal(t, "failed", res.JSON200.Items[0].Status)
	})

	t.Run("filter by plan_only mode returns only plan_only deployment", func(t *testing.T) {
		res, err := client.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{
			ProjectId: ref.Ref(env.ProjectId),
			EnvId:     ref.Ref(env.Id),
			ByMode:    &[]string{string(serverclient.ListDeploymentsParamsByModePlanOnly)},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Len(t, res.JSON200.Items, 1, "expected 1 deployment with plan_only mode")
		assert.Equal(t, dep2.Id, res.JSON200.Items[0].Id)

		// API returns mode="deploy" with plan_only=true for plan-only deployments
		assert.Equal(t, "deploy", res.JSON200.Items[0].Mode)
		assert.True(t, res.JSON200.Items[0].PlanOnly)
	})
}

func generateHashedRunnerToken(orgID, deploymentID string) string {
	secret := os.Getenv("RUNNER_TOKEN_SALT")
	h := sha256.New()
	_, _ = fmt.Fprint(h, secret, orgID, deploymentID)
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

func checkBundleContent(gzipBuf []byte, t *testing.T) map[string]string {
	gr, err := gzip.NewReader(bytes.NewReader(gzipBuf))
	require.NoError(t, err)
	defer func() {
		_ = gr.Close()
	}()

	tr := tar.NewReader(gr)
	var output = map[string]string{}

	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		content, err := io.ReadAll(tr)
		require.NoError(t, err)
		output[header.Name] = string(content)
	}

	return output
}

func TestDeploymentTofu(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	dbConn := MustDatabaseConn(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	runnerId := MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "env-1")

	for _, r := range []platformorchestratorcp.ResourceTypeCreateBody{
		{Id: "k8s-namespace", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}}}},
		{Id: "k8s-secret", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}}}},
		{Id: "postgres", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"conn": map[string]interface{}{"type": "string"}}}},
	} {
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, r)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	for _, d := range []platformorchestratorcp.ModuleCreateBody{
		{
			Id: "default-k8s-namespace", ResourceType: "k8s-namespace", ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`
output "name" {
  value = "example"
}
`),
		},
		{
			Id: "default-k8s-secret", ResourceType: "k8s-secret", ModuleSource: "inline",
			ModuleParams: map[string]platformorchestratorcp.ModuleParamItem{"data": {Type: "map"}},
			ModuleSourceCode: ref.Ref(`variable "data" {
  type = map
}
output "name" {
  value = "example"
}`),
		},
		{
			Id: "default-postgres", ResourceType: "postgres", ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`output "conn" {
  value = "example"
}`),
		},
	} {
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, d)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	for _, r := range []platformorchestratorcp.RuleCreateBody{
		{ModuleId: "default-k8s-namespace"},
		{ModuleId: "default-k8s-secret"},
		{ModuleId: "default-postgres"},
	} {
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, r)
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	t.Run("can dry run deployment", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			IsDryRun:  true,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"ns": {Type: "k8s-namespace"},
							"pg": {Type: "postgres"},
							"pg_conn_secret": {Type: "k8s-secret", Params: map[string]interface{}{
								"data": map[string]interface{}{
									"conn": "${resources.pg.outputs.conn}",
								},
							}},
						},
						Outputs: map[string]string{
							"NAMESPACE": "${resources.ns.outputs.name}",
							"PG_CONN":   "${resources.pg_conn_secret.outputs.name}",
							"COMBINED":  "ns=${resources.ns.outputs.name},conn=${resources.pg.outputs.conn}",
						},
					},
				},
			},
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			assert.Equal(t, runnerId, res.JSON200.RunnerId)
			MakeDiffDeterministic(res.JSON200.Diff)
			assert.Equal(t, []serverclient.DeploymentDiffChange{
				{Id: "<node-hash>", Resource: "workload.default@sample", Summary: "workload added", Type: "added"},
				{Id: "<node-hash>", Resource: "k8s-namespace.default@workloads.sample.ns", Summary: "add resource using module default-k8s-namespace@<module-version-uuid> (dependency of workload.default@sample)", Type: "added"},
				{Id: "<node-hash>", Resource: "postgres.default@workloads.sample.pg", Summary: "add resource using module default-postgres@<module-version-uuid> (dependency of workload.default@sample)", Type: "added"},
				{Id: "<node-hash>", Resource: "k8s-secret.default@workloads.sample.pg_conn_secret", Summary: "add resource using module default-k8s-secret@<module-version-uuid> (dependency of workload.default@sample)", Type: "added"},
			}, res.JSON200.Diff.Changes)
			assert.Empty(t, res.JSON200.Diff.FromDeploymentId)
			assert.Empty(t, res.JSON200.Diff.ToDeploymentId)
		}
	})

	var dep serverclient.Deployment
	t.Run("can create deployment", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"ns": {Type: "k8s-namespace"},
							"pg": {Type: "postgres"},
							"pg_conn_secret": {Type: "k8s-secret", Params: map[string]interface{}{
								"data": map[string]interface{}{
									"conn": "${resources.pg.outputs.conn}",
								},
							}},
						},
						Outputs: map[string]string{
							"NAMESPACE": "${resources.ns.outputs.name}",
							"PG_CONN":   "${resources.pg_conn_secret.outputs.name}",
							"COMBINED":  "ns=${resources.ns.outputs.name},conn=${resources.pg.outputs.conn}",
						},
					},
				},
			},
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
			dep = *res.JSON201
			assert.NotEmpty(t, dep.Id)
			assert.Equal(t, runnerId, res.JSON201.RunnerId)
		}
	})

	t.Run("can extract tofu file", func(t *testing.T) {
		rawTofu := MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid)
		assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
  }
}

module "k8s-namespace_default_workloadssamplens_b55c1617" {
  source = "./modules/default-k8s-namespace/<module-version-uuid>"
}

module "postgres_default_workloadssamplepg_73a31efd" {
  source = "./modules/default-postgres/<module-version-uuid>"
}

module "k8s-secret_default_workloadssamplepg_conn_secret_2f20f708" {
  source = "./modules/default-k8s-secret/<module-version-uuid>"

  data = {
    conn = module.postgres_default_workloadssamplepg_73a31efd.conn
  }

  depends_on = [module.postgres_default_workloadssamplepg_73a31efd]
}

output "platform_orchestrator_metadata" {
  value = {
    "<node-hash>" = lookup(module.k8s-namespace_default_workloadssamplens_b55c1617, "platform_orchestrator_metadata", {})
    "<node-hash>" = lookup(module.postgres_default_workloadssamplepg_73a31efd, "platform_orchestrator_metadata", {})
    "<node-hash>" = lookup(module.k8s-secret_default_workloadssamplepg_conn_secret_2f20f708, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "sample" {
  value = {
    COMBINED  = "ns=${module.k8s-namespace_default_workloadssamplens_b55c1617.name},conn=${module.postgres_default_workloadssamplepg_73a31efd.conn}"
    NAMESPACE = module.k8s-namespace_default_workloadssamplens_b55c1617.name
    PG_CONN   = module.k8s-secret_default_workloadssamplepg_conn_secret_2f20f708.name
  }
  description = "The output variables for workload 'sample'"
  sensitive   = true
}
`,
			rawTofu)
	})

	t.Run("wait for succeeded", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, dpClient, orgId, dep.Id)
		assert.Equal(t, "succeeded", r.Status, r.StatusMessage)
	})

	t.Run("can create another deployment to the same env", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &dep.Manifest,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	})

	t.Run("tofu should be the same", func(t *testing.T) {
		var count int
		require.NoError(t, dbConn.QueryRowContext(t.Context(), "SELECT count(*) FROM (SELECT DISTINCT d.tofu FROM deployments d INNER JOIN deployment_environments e ON d.de_id = e.id WHERE e.org_id = $1) s", dep.OrgId).Scan(&count))
		assert.Equal(t, 1, count)
	})

	t.Run("diff should be empty", func(t *testing.T) {
		r, err := dpClient.CalculateDeploymentDiffWithResponse(t.Context(), orgId, dep.Id, &serverclient.CalculateDeploymentDiffParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		diff := r.JSON200
		assert.Empty(t, diff.Changes)
		assert.Equal(t, &dep.Id, diff.ToDeploymentId)
		assert.NotEmpty(t, diff.FromDeploymentId)
		assert.NotEqual(t, diff.ToDeploymentId, diff.FromDeploymentId)
	})

	t.Run("wait for succeeded", func(t *testing.T) {
		r := MustWaitForDeploymentComplete(t, dpClient, orgId, dep.Id)
		assert.Equal(t, "succeeded", r.Status, r.StatusMessage)
	})

	t.Run("destroy environment creates destroy deployment", func(t *testing.T) {
		res, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, env.ProjectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, res.StatusCode(), string(res.Body))
		require.EventuallyWithTf(t, func(collect *assert.CollectT) {
			res, err := cpClient.GetEnvironmentWithResponse(t.Context(), dep.OrgId, dep.ProjectId, dep.EnvId)
			require.NoError(collect, err)
			require.Equal(collect, http.StatusNotFound, res.StatusCode(), string(res.Body))
		}, time.Minute, time.Second, "env stuck deleting %s/%s/%s", dep.OrgId, env.ProjectId, env.Id)
	})
}

func TestDeploymentModuleUpdate(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "pj-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "default", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.ResourceTypeCreateBody{Id: "thing", OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "thing-def", ResourceType: "thing", ModuleSource: "acme/thing/generic@v1", ModuleInputs: map[string]interface{}{"x": "a"}})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
			def := res.JSON201
			res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: def.Id})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		}
	}

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
					},
				},
			},
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
			dep = *res.JSON201
			res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id,
				&serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())}, serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
		}
	}

	// Update the module definition, redeploy, and ensure the new version was used

	var newModule platformorchestratorcp.Module
	{
		res, err := cpClient.UpdateModuleWithResponse(t.Context(), orgId, "thing-def", platformorchestratorcp.ModuleUpdateBody{ModuleInputs: &map[string]interface{}{"x": "a"}})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			newModule = *res.JSON200
		}
	}

	previousDep := dep
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
					},
				},
			},
		})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body)) {
			dep = *res.JSON201
			res, err := dpClient.UpdateDeploymentResultsWithResponse(t.Context(), dep.OrgId, dep.Id,
				&serverclient.UpdateDeploymentResultsParams{XDeploymentToken: generateHashedRunnerToken(orgId, dep.Id.String())}, serverclient.DeploymentResultsUpdateBody{Status: serverclient.Success})
			require.NoError(t, err)
			require.Equal(t, http.StatusNoContent, res.StatusCode(), string(res.Body))
		}
	}

	// verify the diff shows the module update
	{
		r, err := dpClient.CalculateDeploymentDiffWithResponse(t.Context(), orgId, dep.Id, &serverclient.CalculateDeploymentDiffParams{FromDeploymentId: &previousDep.Id})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		diff := r.JSON200
		MakeDiffDeterministic(*diff)
		assert.Equal(t, []serverclient.DeploymentDiffChange{
			{
				Id: "<node-hash>", Resource: "thing.default@workloads.sample.t",
				Summary: "module changed from thing-def@<module-version-uuid> to thing-def@<module-version-uuid>",
				Type:    serverclient.DeploymentDiffChangeTypeModuleChanged,
			},
		}, diff.Changes)
		assert.Equal(t, 1, diff.NumChanged)
		assert.Equal(t, 0, diff.NumAdded)
		assert.Equal(t, 0, diff.NumRemoved)
		assert.Equal(t, &dep.Id, diff.ToDeploymentId)
		assert.Equal(t, &previousDep.Id, diff.FromDeploymentId)
	}

	{
		res, err := dpClient.ListActiveResourceNodesWithResponse(t.Context(), orgId, &serverclient.ListActiveResourceNodesParams{ProjectId: ref.Ref(env.ProjectId), EnvId: ref.Ref(env.Id)})
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			i := slices.IndexFunc(res.JSON200.Items, func(e serverclient.ActiveResourceNode) bool {
				return e.ResourceType == "thing"
			})
			assert.Equal(t, newModule.VersionId, res.JSON200.Items[i].ModuleVersion)
		}
	}

	// Cannot delete a module definition that is in use

	{
		res, err := cpClient.DeleteModuleWithResponse(t.Context(), orgId, "thing-def")
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body))
		require.Equal(t, "module is used by one or more projects: pj-1", res.JSON409.Message)
	}
}

func Test_CreateDeployment_with_idempotency(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "env-1")

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	t.Run("rejected if manifest is different", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"bananas": {},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body))
		assert.Equal(t, "incorrect manifest or mode for this idempotency key", res.JSON409.Message)
	})

	t.Run("accepted if manifest is the same", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		assert.Equal(t, dep.Id, res.JSON201.Id)
	})
}

func Test_CreateDeployment_with_long_poll(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "k8s-namespace",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}, "secret_name": map[string]interface{}{"type": "string"}}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.CreateModuleProviderJSONRequestBody{Id: "random", ProviderType: "random", VersionConstraint: ">= 3.5.1", Source: "hashicorp/random"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "default-k8s-namespace", ResourceType: "k8s-namespace",
			ModuleSource: "git::https://github.com/delca85/v2-module-sources//definitions/dummy-k8s-namespace", ModuleInputs: map[string]interface{}{"prefix": "${context.project_id}-${context.env_id}", "project": "my-gcp-project"}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "default-k8s-namespace"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var dep serverclient.Deployment
	id, recipient := GetAgeIdentityAndRecipient(t)
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"ns": {Type: "k8s-namespace"},
						},
						Outputs: map[string]string{ //nolint:gosec
							"NAMESPACE":  "${resources.ns.outputs.name}",
							"SECRET_VAR": "${resources.ns.outputs.secret_name}",
						},
					},
				},
			},
			EncryptedOutputsRecipient: ref.Ref(recipient),
			// only for testing purpose we use here the same key, it should be different in real use cases
			EncryptedLogsRecipient: ref.Ref(recipient),
			RunnerLogLevel:         ref.Ref(serverclient.DeploymentCreateBodyRunnerLogLevelDebug),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(t, err) && assert.Equal(c, http.StatusOK, res.StatusCode()) {
			dep = *res.JSON200
			assert.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)

	// Add a small delay to allow logs to be persisted after deployment completion.
	// This prevents a race condition where the test tries to retrieve logs before they're uploaded
	// The runner uploads logs asynchronously after deployment completion, so we need to wait
	time.Sleep(5 * time.Second)

	{
		res, err := dpClient.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
		if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
			decryptedOutputs := decryptOutputs(t, id, []byte(res.JSON200.Raw))
			nsMatch := regexp.MustCompile(`"NAMESPACE":\s*"([^"]*)"`).FindStringSubmatch(decryptedOutputs)
			secretVarMatch := regexp.MustCompile(`"SECRET_VAR":\s*"([^"]*)"`).FindStringSubmatch(decryptedOutputs)
			if assert.Greater(t, len(nsMatch), 1) && assert.Greater(t, len(secretVarMatch), 1) {
				assert.JSONEq(t, fmt.Sprintf(`{
  "sample": {
    "NAMESPACE": "%s",
    "SECRET_VAR": "%s"
  }
}
`, nsMatch[1], secretVarMatch[1]), decryptedOutputs)
			}
		}
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.GetDeploymentLogsWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.GetDeploymentLogsParams{DecryptKey: ref.Ref(id.String())})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Contains(t, string(res.Body), "Plan to create")
	}, 2*time.Minute, 2*time.Second, "deployment logs not pushed after 2 mins for deployment %s", dep.Id)
}

func Test_CreateDeployment_with_long_poll_with_pod_spec(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", &platformorchestratorcp.K8sRunnerJobConfig{
		Namespace:      "platform-orchestrator-runner",
		ServiceAccount: "platform-orchestrator-runner",
		PodTemplate: ref.Ref(map[string]interface{}{
			"metadata": map[string]interface{}{
				"labels": map[string]interface{}{
					"added-via-runner-configuration": "true",
				},
			},
			"spec": map[string]interface{}{
				"containers": []interface{}{
					map[string]interface{}{
						"name": "main",
						"env": []map[string]interface{}{
							{"name": "ADDITIONAL_ENV_VAR", "value": "my-env"},
						},
					},
				},
			},
		}),
	})
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "k8s-namespace",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}, "secret_name": map[string]interface{}{"type": "string"}}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.CreateModuleProviderJSONRequestBody{Id: "random", ProviderType: "random", VersionConstraint: ">= 3.5.1", Source: "hashicorp/random"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "default-k8s-namespace", ResourceType: "k8s-namespace",
			ModuleSource: "git::https://github.com/delca85/v2-module-sources//definitions/dummy-k8s-namespace", ModuleInputs: map[string]interface{}{"project": "my-gcp-project"}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "default-k8s-namespace"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var dep serverclient.Deployment
	id, recipient := GetAgeIdentityAndRecipient(t)
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"ns": {Type: "k8s-namespace"},
						},
						Outputs: map[string]string{ //nolint:gosec
							"NAMESPACE":  "${resources.ns.outputs.name}",
							"SECRET_VAR": "${resources.ns.outputs.secret_name}",
						},
					},
				},
			},
			EncryptedOutputsRecipient: ref.Ref(recipient),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	k8sClient := MustCreateK8sClient(t)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		job, err := k8sClient.BatchV1().Jobs("platform-orchestrator-runner").Get(t.Context(), dep.Id.String(), v1.GetOptions{})
		if assert.NoError(c, err) {
			assert.Equal(c, "true", job.Labels["added-via-runner-configuration"])
			assert.True(c, slices.ContainsFunc(job.Spec.Template.Spec.Containers[0].Env, func(envVar corev1.EnvVar) bool {
				return envVar.Name == "ORG_ID"
			}))
			assert.True(c, slices.ContainsFunc(job.Spec.Template.Spec.Containers[0].Env, func(envVar corev1.EnvVar) bool {
				return envVar.Name == "ADDITIONAL_ENV_VAR"
			}))
		}
	}, 30*time.Second, 2*time.Second, "job not running after 30s, job id: %s", dep.Id)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		require.NoError(c, err)
		if assert.Equal(c, http.StatusOK, res.StatusCode()) {
			assert.Equal(c, "succeeded", res.JSON200.Status, res.JSON200.StatusMessage)
		}
	}, 2*time.Minute, 2*time.Second, "deployment not completed after 30s")

	res, err := dpClient.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
	if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
		decryptedOutputs := decryptOutputs(t, id, []byte(res.JSON200.Raw))
		nsMatch := regexp.MustCompile(`"NAMESPACE":\s*"([^"]*)"`).FindStringSubmatch(decryptedOutputs)
		secretVarMatch := regexp.MustCompile(`"SECRET_VAR":\s*"([^"]*)"`).FindStringSubmatch(decryptedOutputs)
		if assert.Greater(t, len(nsMatch), 1) && assert.Greater(t, len(secretVarMatch), 1) {
			assert.JSONEq(t, fmt.Sprintf(`{
  "sample": {
    "NAMESPACE": "%s",
    "SECRET_VAR": "%s"
  }
}
`, nsMatch[1], secretVarMatch[1]), decryptedOutputs)
		}
	}
}

func decryptOutputs(t *testing.T, id age.Identity, base64Outputs []byte) string {
	if encryptedOutputs, err := base64.StdEncoding.DecodeString(string(base64Outputs)); assert.NoError(t, err) {
		if r, err := age.Decrypt(bytes.NewReader(encryptedOutputs), id); assert.NoError(t, err) {
			out := &bytes.Buffer{}
			if _, err = io.Copy(out, r); assert.NoError(t, err) {
				return out.String()
			}
		}
	}
	return ""
}

func Test_CreateDeployment_with_inline_source_module(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "thing",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "thing-def", ResourceType: "thing",
			ModuleInputs: map[string]interface{}{"number": 42},
			ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`
variable "number" {
	type = number
}

output "answer" {
    value = var.number * 7
}

output "platform_orchestrator_metadata" {
	value = {
         "Positive" = var.number >= 0
    }
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "thing-def"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var dep serverclient.Deployment
	id, recipient := GetAgeIdentityAndRecipient(t)
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
						Outputs: map[string]string{
							"ANSWER": "${resources.t.outputs.answer}",
						},
					},
				},
			},
			EncryptedOutputsRecipient: ref.Ref(recipient),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		require.NoError(c, err)
		if assert.Equal(c, http.StatusOK, res.StatusCode()) {
			require.Equal(c, "succeeded", res.JSON200.Status)
		}
	}, 2*time.Minute, 2*time.Second, "deployment not completed after 30s")

	res, err := dpClient.GetDeploymentEncryptedOutputsWithResponse(t.Context(), orgId, dep.Id)
	if assert.NoError(t, err) && assert.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body)) {
		decryptedOutputs := decryptOutputs(t, id, []byte(res.JSON200.Raw))
		assert.JSONEq(t, `{"sample":{"ANSWER":294}}`, decryptedOutputs)
	}
}

func TestCreateDeployment_with_selector_placeholders(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	dnsType := MustCreateResourceType(t, cpClient, orgId, "dns")
	routeType := MustCreateResourceType(t, cpClient, orgId, "route")
	ingressType := MustCreateResourceType(t, cpClient, orgId, "ingress")

	_ = MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           dnsType.Id + "-default",
		ResourceType: dnsType.Id,
		Coprovisioned: []platformorchestratorcp.ModuleCoProvisionManifest{{
			Type:                 ingressType.Id,
			IsDependentOnCurrent: true,
		}},
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`output "host" {
  value = "localhost"
}`),
	})
	_ = MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           routeType.Id + "-default",
		ResourceType: routeType.Id,
		ModuleParams: map[string]platformorchestratorcp.ModuleParamItem{
			"host": {Type: "string"},
			"path": {Type: "string"},
			"port": {Type: "number"},
		},
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`
variable "path" {
  type = string
}
variable "port" {
  type = number
}
variable "host" {
  type = string
}
output "config" {
  value = {
    path = var.path
    port = var.port
    host = var.host
  }
}
`),
	})
	_ = MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           ingressType.Id + "-default",
		ResourceType: ingressType.Id,
		ModuleInputs: map[string]interface{}{
			"route_configs": "${select.dependencies('dns').consumers('route').outputs.config}",
		},
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`
variable "route_configs" {
  type = list(map(any))
}

locals {
  intermediate = { for _, item in var.route_configs : item.host => item... }
}

output "mapping" {
  value = {
    for host, items in local.intermediate : host => { for item in items : item.path => item.port }
  }
}
`),
	})

	outputs := MustRunDeploymentAndGetOutputs(t, dpClient, orgId, env.ProjectId, env.Id, serverclient.DeploymentManifest{
		Workloads: map[string]serverclient.DeploymentManifestWorkload{
			"sample": {
				Resources: map[string]serverclient.DeploymentManifestResource{
					"dns": {
						Type: dnsType.Id,
						Id:   ref.Ref("dns-default.eg"),
					},
					"route1": {
						Type: routeType.Id,
						Params: map[string]interface{}{
							"host": "${resources.dns.outputs.host}",
							"path": "/some/path",
							"port": 8080,
						},
					},
					"route2": {
						Type: routeType.Id,
						Params: map[string]interface{}{
							"host": "${resources.dns.outputs.host}",
							"path": "/other/path",
							"port": 8081,
						},
					},
					"ingress": {
						Type: ingressType.Id,
						Id:   ref.Ref("dns-default.eg"),
					},
				},
				Outputs: map[string]string{
					"output": "${resources.ingress.outputs.mapping}",
				},
			},
		},
	})
	require.Equal(t, map[string]interface{}{
		"sample": map[string]interface{}{
			"output": map[string]interface{}{
				"localhost": map[string]interface{}{
					"/some/path":  "8080",
					"/other/path": "8081",
				},
			},
		},
	}, outputs)
}

func Test_CreateDeployment_and_force_fail(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalDpClient := MustInternalDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "thing",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "thing-def", ResourceType: "thing",
			ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`
resource "terraform_data" "thing" {
  provisioner "local-exec" {
    command = "sleep 3600"
  }
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "thing-def"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"t": {Type: "thing"},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	select {
	case <-time.After(5 * time.Second):
	case <-t.Context().Done():
	}

	r, err := internalDpClient.InternalForceFailDeploymentWithResponse(t.Context(), dep.OrgId, dep.Id)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, r.StatusCode())
	assert.Equal(t, "failed", r.JSON200.Status)
	assert.Equal(t, "deployment was manually failed by an operator", r.JSON200.StatusMessage)
}

func TestCreateDeployment_fail_due_to_module_params(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	thingType := MustCreateResourceType(t, cpClient, orgId, "thing")

	mod := MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           thingType.Id + "-default",
		ResourceType: thingType.Id,
		ModuleSource: "inline",
		ModuleParams: map[string]platformorchestratorcp.ModuleParamItem{
			"animal": {Type: platformorchestratorcp.String},
		},
		ModuleSourceCode: ref.Ref(`output "host" {
  value = "localhost"
}`),
	})

	res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
		ProjectId: projectId, EnvId: env.Id,
		Mode: serverclient.DeploymentCreateBodyModeDeploy,
		Manifest: &serverclient.DeploymentManifest{
			Workloads: map[string]serverclient.DeploymentManifestWorkload{
				"sample": {
					Resources: map[string]serverclient.DeploymentManifestResource{
						"t": {
							Type: thingType.Id,
							Params: map[string]interface{}{
								"fruit": "banana",
							},
						},
					},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
	assert.Equal(t, "node 'type=thing,class=default,id=workloads.sample.t': uses module thing-default@"+mod.VersionId+" which requires string param 'animal' to be set", res.JSON400.Message)
}

func Test_CreateDeployment_with_removed_nodes_with_providers(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")
	t.Log("using org", orgId, "project", projectId, "env", env.Id)

	p, _ := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleProviderCreateBody{
		ProviderType: "faulty", Id: "default", Source: "astromechza/faulty", VersionConstraint: "= 0.1.0",
		Configuration: map[string]interface{}{
			"required_boolean": true,
		},
	})
	require.Equal(t, http.StatusCreated, p.StatusCode())

	rt := MustCreateResourceType(t, cpClient, orgId, "example")
	MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:              rt.Id + "-default",
		ResourceType:    rt.Id,
		ProviderMapping: map[string]string{"faulty": "faulty.default"},
		ModuleSource:    "inline",
		ModuleSourceCode: ref.Ref(`
terraform {
  required_providers {
    faulty = {
      source  = "astromechza/faulty"
	}
  }
}

resource "faulty_example" "eg" {
  required_boolean = true
}
`),
	})

	_ = MustRunDeploymentAndGetOutputs(t, dpClient, orgId, env.ProjectId, env.Id, serverclient.DeploymentManifest{
		Workloads: map[string]serverclient.DeploymentManifestWorkload{
			"sample": {
				Resources: map[string]serverclient.DeploymentManifestResource{
					"t": {
						Type: rt.Id,
					},
				},
			},
		},
	})

	_ = MustRunDeploymentAndGetOutputs(t, dpClient, orgId, env.ProjectId, env.Id, serverclient.DeploymentManifest{
		Workloads: map[string]serverclient.DeploymentManifestWorkload{
			"sample": {
				Resources: map[string]serverclient.DeploymentManifestResource{},
			},
		},
	})
}

func Test_CreateDeployment_with_long_poll_fail_job_stuck(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)
	cpClient := MustControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "project-1").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "test-runner", "", "", ref.Ref(platformorchestratorcp.K8sRunnerJobConfig{
		Namespace:      "platform-orchestrator-runner",
		ServiceAccount: "not-existing",
	})).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "env-1")

	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.CreateResourceTypeJSONRequestBody{Id: "k8s-namespace",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"name": map[string]interface{}{"type": "string"}, "secret_name": map[string]interface{}{"type": "string"}}}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.CreateModuleProviderJSONRequestBody{Id: "random", ProviderType: "random", VersionConstraint: ">= 3.5.1", Source: "hashicorp/random"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{Id: "default-k8s-namespace", ResourceType: "k8s-namespace",
			ModuleSource: "git::https://github.com/delca85/v2-module-sources//definitions/dummy-k8s-namespace", ModuleInputs: map[string]interface{}{"prefix": "${context.project_id}-${context.env_id}", "project": "my-gcp-project"}})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{ModuleId: "default-k8s-namespace"})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	var dep serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{
			IdempotencyKey: ref.Ref("hello-world"),
		}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"sample": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"ns": {Type: "k8s-namespace"},
						},
						Outputs: map[string]string{ //nolint:gosec
							"NAMESPACE":  "${resources.ns.outputs.name}",
							"SECRET_VAR": "${resources.ns.outputs.secret_name}",
						},
					},
				},
			},
			RunnerLogLevel: ref.Ref(serverclient.DeploymentCreateBodyRunnerLogLevelDebug),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = *res.JSON201
	}

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		res, err := dpClient.WaitForDeploymentCompleteWithResponse(t.Context(), dep.OrgId, dep.Id, &serverclient.WaitForDeploymentCompleteParams{})
		if assert.NoError(t, err) && assert.Equal(c, http.StatusOK, res.StatusCode()) {
			assert.Equal(c, "failed", res.JSON200.Status)
			assert.Contains(c, res.JSON200.StatusMessage, "error looking up service account platform-orchestrator-runner/not-existing: serviceaccount \"not-existing\" not found")
		}
	}, 2*time.Minute, 2*time.Second, "deployment %s not completed after 2 mins: %s", dep.Id, dep.StatusMessage)
}

func TestDeployment_with_static_provider_flow(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "env-1")

	var provider *platformorchestratorcp.ModuleProvider
	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleProviderCreateBody{
			ProviderType: "faulty", Id: "default", Source: "astromechza/faulty", VersionConstraint: "= 0.1.0",
			Configuration: map[string]interface{}{
				"required_boolean": true,
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		provider = res.JSON201
	}
	rt := MustCreateResourceType(t, cpClient, orgId, "example")
	MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:              rt.Id + "-default",
		ResourceType:    rt.Id,
		ProviderMapping: map[string]string{"faulty": "faulty.default"},
		ModuleSource:    "inline",
		ModuleSourceCode: ref.Ref(`
terraform {
	required_providers {
		faulty = {
			source  = "astromechza/faulty"
		}
	}
}
resource "faulty_example" "eg" {
	required_boolean = true
}
output "platform_orchestrator_metadata" {
	value = {}
}`),
	})

	var manifestResources = map[string]serverclient.DeploymentManifestResource{
		"eg": {Type: rt.Id},
	}

	// THEN do a new deployment with the resource, this should succeed just fine
	var dep *serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {Resources: manifestResources}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status, dep.StatusMessage)

	// Break the provider
	{
		res, err := cpClient.UpdateModuleProviderWithResponse(t.Context(), orgId, provider.ProviderType, provider.Id, platformorchestratorcp.ModuleProviderUpdateBody{
			Configuration: &map[string]interface{}{},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	}

	// THEN do a new deployment with the same manifest, this should fail due to bad provider config
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &dep.Manifest,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "failed", dep.Status)
	var statusMessageError map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(dep.StatusMessage), &statusMessageError))
	require.Equal(t, "The argument \"required_boolean\" is required, but no definition was found.", statusMessageError["detail"])
	require.Equal(t, "faulty", statusMessageError["provider_type"])
	require.Equal(t, "default", statusMessageError["provider_id"])
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
    faulty-default-3e5e137e = {
      source  = "astromechza/faulty"
      version = "= 0.1.0"
    }
  }
}

provider "faulty-default-3e5e137e" {
  alias = "faulty-default-3e5e137e"
}

module "example_default_workloadsmaineg_d2bc5d89" {
  source = "./modules/example-default/<module-version-uuid>"
  providers = {
    faulty = faulty-default-3e5e137e.faulty-default-3e5e137e
  }
}

output "platform_orchestrator_metadata" {
  value = {
    "<node-hash>" = lookup(module.example_default_workloadsmaineg_d2bc5d89, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "main" {
  value       = {}
  description = "The output variables for workload 'main'"
  sensitive   = true
}
`, MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid))

	// Fix the provider
	{
		res, err := cpClient.UpdateModuleProviderWithResponse(t.Context(), orgId, provider.ProviderType, provider.Id, platformorchestratorcp.ModuleProviderUpdateBody{
			Configuration: &map[string]interface{}{"required_boolean": true},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	}

	// Retry this again! This time it should succeed
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &dep.Manifest,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status)

	// Break the provider again
	{
		res, err := cpClient.UpdateModuleProviderWithResponse(t.Context(), orgId, provider.ProviderType, provider.Id, platformorchestratorcp.ModuleProviderUpdateBody{
			Configuration: &map[string]interface{}{},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	}

	// THEN do a new deployment with an empty manifest, this should fail due to bad provider config
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "failed", dep.Status)
	assert.Contains(t, dep.StatusMessage, "The argument \\\"required_boolean\\\" is required, but no definition was found.")
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
    faulty-default-3e5e137e = {
      source  = "astromechza/faulty"
      version = "= 0.1.0"
    }
  }
}

provider "faulty-default-3e5e137e" {
  alias = "faulty-default-3e5e137e"
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "main" {
  value       = {}
  description = "The output variables for workload 'main'"
  sensitive   = true
}
`, MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid))

	// Fix the provider again
	{
		res, err := cpClient.UpdateModuleProviderWithResponse(t.Context(), orgId, provider.ProviderType, provider.Id, platformorchestratorcp.ModuleProviderUpdateBody{
			Configuration: &map[string]interface{}{"required_boolean": true},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	}

	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &dep.Manifest,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
    faulty-default-3e5e137e = {
      source  = "astromechza/faulty"
      version = "= 0.1.0"
    }
  }
}

provider "faulty-default-3e5e137e" {
  alias            = "faulty-default-3e5e137e"
  required_boolean = true
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "main" {
  value       = {}
  description = "The output variables for workload 'main'"
  sensitive   = true
}
`, MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid))

	// Now let's destroy this env and check the tofu
	{
		res, err := cpClient.DeleteEnvironmentWithResponse(t.Context(), orgId, env.ProjectId, env.Id, &platformorchestratorcp.DeleteEnvironmentParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusAccepted, res.StatusCode(), string(res.Body))
	}
	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		res, err := dpClient.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{ProjectId: ref.Ref(env.ProjectId), EnvId: ref.Ref(env.Id)})
		require.NoError(collect, err)
		require.Equal(collect, http.StatusOK, res.StatusCode(), string(res.Body))
		require.Equal(collect, "destroy", res.JSON200.Items[0].Mode)
		dep.Id = res.JSON200.Items[0].Id
	}, time.Minute, time.Second, "failed to find destroy deployment")
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
    faulty-default-3e5e137e = {
      source  = "astromechza/faulty"
      version = "= 0.1.0"
    }
  }
}

provider "faulty-default-3e5e137e" {
  alias            = "faulty-default-3e5e137e"
  required_boolean = true
}

output "platform_orchestrator_metadata" {
  value       = {}
  description = "The metadata output from the modules involved in the deployment"
}

output "main" {
  value       = {}
  description = "The output variables for workload 'main'"
  sensitive   = true
}
`, MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid))
}

func TestDeployment_with_deleted_dynamic_provider_flow(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "env-1")

	parentRt := MustCreateResourceType(t, cpClient, orgId, "parent")
	MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           parentRt.Id,
		ResourceType: parentRt.Id,
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`
output "boolean" {
    value = true
}
`),
	})

	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleProviderCreateBody{
			ProviderType: "faulty", Id: "default", Source: "astromechza/faulty", VersionConstraint: "= 0.1.0",
			Configuration: map[string]interface{}{
				"required_boolean": "${resources.parent.outputs.boolean}",
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	childRt := MustCreateResourceType(t, cpClient, orgId, "child")
	childModule := MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:              childRt.Id,
		ResourceType:    childRt.Id,
		ProviderMapping: map[string]string{"faulty": "faulty.default"},
		Dependencies: map[string]platformorchestratorcp.ModuleDependencyManifest{
			"parent": {Type: parentRt.Id},
		},
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`
terraform {
	required_providers {
		faulty = {
			source  = "astromechza/faulty"
		}
	}
}
resource "faulty_example" "eg" {
	required_boolean = true
}
`),
	})

	// THEN do a new deployment with the resource, this should succeed just fine
	var dep *serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {Resources: map[string]serverclient.DeploymentManifestResource{"eg": {Type: childRt.Id}}}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status)

	// THEN do a new deployment with an empty manifest, this should fail
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
		assert.Equal(
			t,
			"provider 'faulty.default' for module 'child@"+childModule.VersionId+"' in context of node 'type=child,class=default,id=workloads.main.eg': failed to resolve placeholders: placeholder '${resources.parent.outputs.boolean}' resolves to a deleted node 'type=parent,class=default,id=workloads.main.eg' which must be anchored in the graph",
			res.JSON400.Message,
		)
	}

	// Rollback to the previous deployment to re-anchor the parent node
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId:              env.ProjectId,
			EnvId:                  env.Id,
			Mode:                   serverclient.DeploymentCreateBodyModeRollback,
			RollbackToDeploymentId: ref.Ref(dep.Id),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
		dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
		require.Equal(t, "succeeded", dep.Status)
	}

	// So we must anchor the parent node to the graph first
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{
				"main": {
					Resources: map[string]serverclient.DeploymentManifestResource{
						"eg2": {Type: parentRt.Id, Id: ref.Ref("workloads.main.eg")},
					},
				},
			}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status)
	assert.Equal(t, `terraform {
  backend "kubernetes" {
    secret_suffix     = "<env-uuid>"
    namespace         = "platform-orchestrator-runner"
    in_cluster_config = true
  }
  required_providers {
    faulty-default-dbd0d8cb = {
      source  = "astromechza/faulty"
      version = "= 0.1.0"
    }
  }
}

module "parent_default_workloadsmaineg_d22ed391" {
  source = "./modules/parent/<module-version-uuid>"
}

provider "faulty-default-dbd0d8cb" {
  alias            = "faulty-default-dbd0d8cb"
  required_boolean = module.parent_default_workloadsmaineg_d22ed391.boolean
}

output "platform_orchestrator_metadata" {
  value = {
    "<node-hash>" = lookup(module.parent_default_workloadsmaineg_d22ed391, "platform_orchestrator_metadata", {})
  }
  description = "The metadata output from the modules involved in the deployment"
}

output "main" {
  value       = {}
  description = "The output variables for workload 'main'"
  sensitive   = true
}
`, MakeTofuDeterministic(t, MustGetDeploymentTofu(t, dpClient, orgId, dep.Id), env.Uuid))

	// Then we can finally do a deployment with the empty manifest
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status)
}

func TestDeployment_rollback_flow(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "app-1").Id
	MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil)
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "env-1")

	resType := MustCreateResourceType(t, cpClient, orgId, "thing")
	MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:           resType.Id,
		ResourceType: resType.Id,
		ModuleSource: "inline",
		ModuleSourceCode: ref.Ref(`
output "boolean" {
    value = true
}
`),
	})

	t.Run("cannot do a rollback without rollback target", func(t *testing.T) {
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeRollback,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusBadRequest, res.StatusCode(), string(res.Body))
		assert.Equal(t, "rollback_to_deployment_id must be set when mode is rollback", res.JSON400.Message)
	})

	t.Run("cannot do a rollback with missing rollback target", func(t *testing.T) {
		rollbackDepId := uuid.New()
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId:              env.ProjectId,
			EnvId:                  env.Id,
			Mode:                   serverclient.DeploymentCreateBodyModeRollback,
			RollbackToDeploymentId: ref.Ref(rollbackDepId),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusConflict, res.StatusCode(), string(res.Body))
		assert.Equal(t, fmt.Sprintf("deployment '%s' not found", rollbackDepId), res.JSON409.Message)
	})

	// THEN do a new deployment with the resource, this should succeed just fine
	var rollbackDep *serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{Workloads: map[string]serverclient.DeploymentManifestWorkload{"main": {
				Resources: map[string]serverclient.DeploymentManifestResource{"eg": {Type: resType.Id}},
				Variables: map[string]string{
					"key": "${resources.eg.outputs.boolean}",
				},
			}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		rollbackDep = res.JSON201
	}
	rollbackDep = MustWaitForDeploymentComplete(t, dpClient, rollbackDep.OrgId, rollbackDep.Id)
	require.Equal(t, "succeeded", rollbackDep.Status, rollbackDep.StatusMessage)

	// Now update the module with a "broken" module source
	{
		res, err := cpClient.UpdateModuleWithResponse(t.Context(), orgId, resType.Id, platformorchestratorcp.ModuleUpdateBody{
			ModuleSourceCode: ref.Ref(`
output "unknown" {
  value = null
}
`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
	}

	// Now do a deployment with the broken module source, this should fail
	var dep *serverclient.Deployment
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest:  &rollbackDep.Manifest,
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "failed", dep.Status)
	assert.Equal(t, "deploy", dep.Mode)
	assert.False(t, dep.PlanOnly)
	var statusMessageError map[string]interface{}
	require.NoError(t, json.Unmarshal([]byte(dep.StatusMessage), &statusMessageError), string(dep.StatusMessage))
	require.Equal(t, "Unsupported attribute", statusMessageError["summary"])
	require.Equal(t, "This object does not have an attribute named \"boolean\".", statusMessageError["detail"])
	require.Equal(t, "main", statusMessageError["workload"])

	// check the diff
	{
		r, err := dpClient.CalculateDeploymentDiffWithResponse(t.Context(), orgId, dep.Id, &serverclient.CalculateDeploymentDiffParams{FromDeploymentId: &rollbackDep.Id})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		diff := r.JSON200
		MakeDiffDeterministic(*diff)
		assert.Equal(t, []serverclient.DeploymentDiffChange{{
			Id: "<node-hash>", Resource: "thing.default@workloads.main.eg", Type: "module_changed",
			Summary: "module changed from thing@<module-version-uuid> to thing@<module-version-uuid>",
		}}, diff.Changes)
		assert.Equal(t, 1, diff.NumChanged)
		assert.Equal(t, 0, diff.NumAdded)
		assert.Equal(t, 0, diff.NumRemoved)
		assert.Equal(t, &dep.Id, diff.ToDeploymentId)
		assert.Equal(t, &rollbackDep.Id, diff.FromDeploymentId)
	}

	// Now do a rollback to the previous deployment, this should succeed
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId:              env.ProjectId,
			EnvId:                  env.Id,
			Mode:                   serverclient.DeploymentCreateBodyModeRollback,
			RollbackToDeploymentId: ref.Ref(rollbackDep.Id),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status, dep.StatusMessage)
	assert.Equal(t, "rollback", dep.Mode)
	assert.False(t, dep.PlanOnly)
	if assert.NotEmpty(t, dep.RollbackToDeploymentId) {
		assert.Equal(t, rollbackDep.Id, *dep.RollbackToDeploymentId)
	}

	// check that the diff is reversed now
	{
		r, err := dpClient.CalculateDeploymentDiffWithResponse(t.Context(), orgId, dep.Id, &serverclient.CalculateDeploymentDiffParams{})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, r.StatusCode(), string(r.Body))
		diff := r.JSON200
		MakeDiffDeterministic(*diff)
		assert.Equal(t, []serverclient.DeploymentDiffChange{{
			Id: "<node-hash>", Resource: "thing.default@workloads.main.eg", Type: "module_changed",
			Summary: "module changed from thing@<module-version-uuid> to thing@<module-version-uuid>",
		}}, diff.Changes)
		assert.Equal(t, 1, diff.NumChanged)
		assert.Equal(t, 0, diff.NumAdded)
		assert.Equal(t, 0, diff.NumRemoved)
		assert.Equal(t, &dep.Id, diff.ToDeploymentId)
		assert.NotEmpty(t, diff.FromDeploymentId)
	}

	// check that we get the rollback id on Get
	{
		res, err := dpClient.GetDeploymentWithResponse(t.Context(), orgId, dep.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		assert.Equal(t, dep.Id, res.JSON200.Id)
		assert.Equal(t, "rollback", res.JSON200.Mode)
		assert.False(t, dep.PlanOnly)
		if assert.NotEmpty(t, res.JSON200.RollbackToDeploymentId) {
			assert.Equal(t, rollbackDep.Id, *res.JSON200.RollbackToDeploymentId)
		}
	}

	// check that we see the rollback id on list
	{
		res, err := dpClient.ListDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListDeploymentsParams{ProjectId: &dep.ProjectId, EnvId: &dep.EnvId})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		if assert.NotEmpty(t, res.JSON200) && assert.NotEmpty(t, res.JSON200.Items) {
			i := (*res.JSON200).Items[0]
			assert.Equal(t, dep.Id, i.Id)
			assert.Equal(t, "rollback", i.Mode)
			assert.False(t, dep.PlanOnly)
			if assert.NotEmpty(t, i.RollbackToDeploymentId) {
				assert.Equal(t, rollbackDep.Id, *i.RollbackToDeploymentId)
			}
		}
	}

	// check that we see the rollback id on list-last
	{
		res, err := dpClient.ListLastDeploymentsWithResponse(t.Context(), orgId, &serverclient.ListLastDeploymentsParams{ProjectId: &dep.ProjectId, EnvId: &dep.EnvId})
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))
		if assert.NotEmpty(t, res.JSON200) && assert.NotEmpty(t, res.JSON200.Items) {
			i := (*res.JSON200).Items[0]
			assert.Equal(t, dep.Id, i.Id)
			assert.Equal(t, "rollback", i.Mode)
			assert.False(t, dep.PlanOnly)
			if assert.NotEmpty(t, i.RollbackToDeploymentId) {
				assert.Equal(t, rollbackDep.Id, *i.RollbackToDeploymentId)
			}
		}
	}

	// Now do a rollback to the previous deployment, this should succeed
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId:              env.ProjectId,
			EnvId:                  env.Id,
			Mode:                   serverclient.DeploymentCreateBodyModeRollback,
			PlanOnly:               ref.Ref(true),
			RollbackToDeploymentId: ref.Ref(dep.Id),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
	}
	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "succeeded", dep.Status, dep.StatusMessage)
	assert.Equal(t, "rollback", dep.Mode)
	assert.True(t, dep.PlanOnly)
}

func TestDeployment_ModuleErrorEnrichment(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "test-app").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "test-env")

	// Create a resource type
	{
		res, err := cpClient.CreateResourceTypeWithResponse(t.Context(), orgId, platformorchestratorcp.ResourceTypeCreateBody{
			Id:           "database",
			OutputSchema: map[string]interface{}{"type": "object", "properties": map[string]interface{}{"connection_string": map[string]interface{}{"type": "string"}}},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	// Create a module with a REQUIRED parameter (non-optional)
	var moduleId, moduleVersionId string
	{
		res, err := cpClient.CreateModuleWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleCreateBody{
			Id:           "database-module",
			ResourceType: "database",
			ModuleSource: "inline",
			ModuleSourceCode: ref.Ref(`variable "db_name" {
  type = string
}

output "connection_string" {
  value = "postgres://localhost/${var.db_name}"
}`),
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		moduleId = res.JSON201.Id
		moduleVersionId = res.JSON201.VersionId
		t.Logf("Created module %s version %s", moduleId, moduleVersionId)
	}

	// Create a rule for the module
	{
		res, err := cpClient.CreateModuleRuleInOrgWithResponse(t.Context(), orgId, platformorchestratorcp.RuleCreateBody{
			ModuleId: "database-module",
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	}

	t.Run("deployment fails with missing required module input and error is enriched", func(t *testing.T) {
		// Create a deployment that uses the module but DOESN'T provide the required parameter
		var dep *serverclient.Deployment
		{
			res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
				ProjectId: env.ProjectId,
				EnvId:     env.Id,
				Mode:      serverclient.DeploymentCreateBodyModeDeploy,
				Manifest: &serverclient.DeploymentManifest{
					Workloads: map[string]serverclient.DeploymentManifestWorkload{
						"api": {
							Resources: map[string]serverclient.DeploymentManifestResource{
								"db": {
									Type: "database",
								},
							},
						},
					},
				},
			})
			require.NoError(t, err)
			require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
			dep = res.JSON201
			t.Logf("Created deployment %s", dep.Id)
		}

		dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
		require.Equal(t, "failed", dep.Status)

		// Fetch the deployment and verify the error message was enriched
		{
			res, err := dpClient.GetDeploymentWithResponse(t.Context(), orgId, dep.Id)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))

			dep := res.JSON200
			assert.Equal(t, "failed", dep.Status)
			t.Logf("Deployment status message: %s", dep.StatusMessage)

			// Parse the status message as JSON and verify it contains enriched information
			var enrichedError map[string]interface{}
			err = json.Unmarshal([]byte(dep.StatusMessage), &enrichedError)
			require.NoError(t, err, "Status message should be valid JSON")

			// Verify that the error was enriched with module information
			assert.Equal(t, "database-module", enrichedError["module_id"], "Error should be enriched with module_id")
			assert.Equal(t, moduleVersionId, enrichedError["module_version"], "Error should be enriched with module_version")
			assert.Contains(t, enrichedError["entity_id"], "database_default_workloadsapidb")
			assert.Equal(t, "module", enrichedError["entity_type"])
			assert.Equal(t, "The argument \"db_name\" is required, but no definition was found.", enrichedError["detail"])
			assert.Equal(t, "Missing required argument", enrichedError["summary"])

			t.Logf("✅ Error was successfully enriched with module_id=%s and module_version=%s", enrichedError["module_id"], enrichedError["module_version"])
		}
	})
}

func TestDeployment_ProviderErrorEnrichment(t *testing.T) {
	t.Parallel()
	dpClient := MustDataPlaneClient(t)
	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)
	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	appId := MustCreateProject(t, cpClient, orgId, "test-app").Id
	_ = MustCreateRunnerWithRule(t, cpClient, orgId, "default", appId, envType, nil).Id
	env := MustCreateEnv(t, cpClient, orgId, envType, appId, "test-env")

	// Create a resource type
	rt := MustCreateResourceType(t, cpClient, orgId, "storage")

	// Create a provider with correct configuration
	var provider *platformorchestratorcp.ModuleProvider
	{
		res, err := cpClient.CreateModuleProviderWithResponse(t.Context(), orgId, platformorchestratorcp.ModuleProviderCreateBody{
			ProviderType:      "faulty",
			Id:                "storage-provider",
			Source:            "astromechza/faulty",
			VersionConstraint: "= 0.1.0",
			Configuration:     map[string]interface{}{},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		provider = res.JSON201
		t.Logf("Created provider %s.%s", provider.ProviderType, provider.Id)
	}

	_ = MustCreateModuleAndRule(t, cpClient, orgId, platformorchestratorcp.ModuleCreateBody{
		Id:              "storage-module",
		ResourceType:    rt.Id,
		ProviderMapping: map[string]string{"faulty": "faulty.storage-provider"},
		ModuleSource:    "inline",
		ModuleSourceCode: ref.Ref(`
terraform {
	required_providers {
		faulty = {
			source  = "astromechza/faulty"
		}
	}
}

resource "faulty_example" "storage" {
	required_boolean = true
}

output "platform_orchestrator_metadata" {
	value = {}
}

output "storage_path" {
	value = "/storage"
}`),
	})

	var dep *serverclient.Deployment

	// Create a deployment - this should fail due to bad provider config
	{
		res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
			ProjectId: env.ProjectId,
			EnvId:     env.Id,
			Mode:      serverclient.DeploymentCreateBodyModeDeploy,
			Manifest: &serverclient.DeploymentManifest{
				Workloads: map[string]serverclient.DeploymentManifestWorkload{
					"api": {
						Resources: map[string]serverclient.DeploymentManifestResource{
							"store": {
								Type: rt.Id,
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
		dep = res.JSON201
		t.Logf("Created deployment %s", dep.Id)
	}

	dep = MustWaitForDeploymentComplete(t, dpClient, dep.OrgId, dep.Id)
	require.Equal(t, "failed", dep.Status)

	// Verify the error message is enriched with provider information
	{
		res, err := dpClient.GetDeploymentWithResponse(t.Context(), orgId, dep.Id)
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode(), string(res.Body))

		dep := res.JSON200
		assert.Equal(t, "failed", dep.Status)
		t.Logf("Deployment status message: %s", dep.StatusMessage)

		// Parse the status message as JSON and verify it contains enriched information
		var enrichedError map[string]interface{}
		err = json.Unmarshal([]byte(dep.StatusMessage), &enrichedError)
		require.NoError(t, err, "Status message should be valid JSON")

		// Verify that the error was enriched with provider information
		assert.Equal(t, "faulty", enrichedError["provider_type"], "Error should be enriched with provider_type")
		assert.Equal(t, "storage-provider", enrichedError["provider_id"], "Error should be enriched with provider_id")
		assert.Equal(t, "provider", enrichedError["entity_type"])
		assert.Contains(t, enrichedError["detail"], "required_boolean")
		assert.Equal(t, "Missing required argument", enrichedError["summary"])
	}
}
