package integrationtests

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/batch/v1"

	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"

	serverclient "github.com/stellwerk-labs/platform-orchestrator-dp/shared/genclient"
)

func TestWaitAndPushRunnerMessages(t *testing.T) {
	t.Parallel()

	cpClient := MustControlPlaneClient(t)
	internalCpClient := MustInternalControlPlaneClient(t)

	orgId := MustCreateOrgId(t, internalCpClient)
	t.Logf("using org %s", orgId)

	envType := MustCreateEnvType(t, cpClient, orgId, "dev").Id
	projectId := MustCreateProject(t, cpClient, orgId, "my-project").Id
	runner, privateKey := MustCreateRemoteRunnerWithRule(t, cpClient, orgId, "test-runner", projectId, envType)
	env := MustCreateEnv(t, cpClient, orgId, envType, projectId, "dev-env")

	dpClient := MustDataPlaneClient(t)

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	respChan := make(chan *serverclient.WaitForRemoteRunnerMessagesResponse, 2)
	errChan := make(chan error, 1)

	go func() {
		for i := 0; i < 2; i++ {
			resp, err := dpClient.WaitForRemoteRunnerMessagesWithResponse(ctx, orgId, runner.Id, func(ctx context.Context, req *http.Request) error {
				claims := jwt.MapClaims{
					"typ": "JWT",
					"alg": "EdDSA",
					"kid": getPublicKeyFingerprint(privateKey),
				}
				token := signJwt(privateKey, claims)
				req.Header.Set("Authorization", "Bearer "+token)
				return nil
			})
			if err != nil {
				errChan <- err
				return
			}
			respChan <- resp
		}
	}()
	time.Sleep(1 * time.Second) // Give some time for the goroutine to start

	// Create a deployment, this should make the runner receive a message
	res, err := dpClient.CreateDeploymentWithResponse(t.Context(), orgId, &serverclient.CreateDeploymentParams{}, serverclient.CreateDeploymentJSONRequestBody{
		ProjectId: projectId,
		EnvId:     env.Id,
		Mode:      serverclient.DeploymentCreateBodyModeDeploy,
		Manifest: &serverclient.DeploymentManifest{
			Workloads: map[string]serverclient.DeploymentManifestWorkload{
				"sample": {},
			},
		},
		EncryptedOutputsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
		// only for testing purpose we use here the same key, it should be different in real use cases
		EncryptedLogsRecipient: ref.Ref("age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p"),
	})
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, res.StatusCode(), string(res.Body))
	dep := res.JSON201

	var msgCount = 0
	var token string

	// Check the content of the response of WaitForRunnerMessages
	for msgCount < 2 {
		select {
		case resp := <-respChan:
			msgCount++
			require.Equal(t, http.StatusOK, resp.StatusCode(), string(resp.Body))
			require.NotNil(t, resp.JSON200)

			if msgCount == 1 {
				createJob, err := resp.JSON200.AsRemoteRunnerMessageCreateJob()
				require.NoError(t, err)

				require.Equal(t, serverclient.CreateJob, createJob.Action)
				require.Equal(t, dep.Id.String(), createJob.JobId)
				require.Equal(t, "platform-orchestrator-runner", createJob.Namespace)
				require.NotEmpty(t, createJob.Configuration)
				config, _ := json.Marshal(createJob.Configuration)
				var jobSpec v1.JobSpec
				require.NoError(t, json.Unmarshal(config, &jobSpec))

				assert.NotEmpty(t, jobSpec.Template)
				assert.NotEmpty(t, jobSpec.Parallelism)
				assert.NotEmpty(t, jobSpec.TTLSecondsAfterFinished)

				template := jobSpec.Template
				spec := template.Spec
				containers := spec.Containers
				require.Len(t, containers, 1)
				require.Equal(t, "platform-orchestrator-runner", jobSpec.Template.Spec.ServiceAccountName)
				var envVars = map[string]string{}
				for _, v := range containers[0].Env {
					envVars[v.Name] = v.Value
				}
				token = createJob.DeploymentToken
				require.Equal(t, token, envVars["TOKEN"])
				require.Equal(t, "age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p", envVars["ENCRYPTING_KEY"])
				require.NotEmpty(t, envVars["LOGS_URL"])
				require.Equal(t, "info", envVars["LOG_LEVEL"])
			} else {
				checkJobStatus, err := resp.JSON200.AsRemoteRunnerMessageCheckJobStatus()
				require.NoError(t, err)
				require.Equal(t, serverclient.CheckJobStatus, checkJobStatus.Action)
				require.Equal(t, dep.Id.String(), checkJobStatus.JobId)
				require.Equal(t, token, checkJobStatus.DeploymentToken)
				require.Equal(t, "platform-orchestrator-runner", checkJobStatus.Namespace)
				require.Equal(t, dep.CreatedAt.Add(5*time.Second), checkJobStatus.ExpiresAt)
			}

		case err := <-errChan:
			t.Fatalf("WaitForRunnerMessages failed: %v", err)

		case <-ctx.Done():
			t.Fatal("Test timed out waiting for runner message")
		}
	}
}

func signJwt(privateKey []byte, claims map[string]interface{}) string {
	token := jwt.NewWithClaims(jwt.SigningMethodEdDSA, jwt.MapClaims(claims))
	signedToken, _ := token.SignedString(ed25519.PrivateKey(privateKey))

	return signedToken
}

func getPublicKeyFingerprint(privateKey ed25519.PrivateKey) string {
	publicKey := ed25519.PublicKey(privateKey[32:])
	hash := sha256.Sum256(publicKey)
	return hex.EncodeToString(hash[:])
}
