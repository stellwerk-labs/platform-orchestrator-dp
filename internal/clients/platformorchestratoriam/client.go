package platformorchestratoriam

//go:generate go tool mockgen -destination mocks/client_mock.go -package mockplatformorchestratoriam github.com/stellwerk-labs/platform-orchestrator-iam/shared/genclient ClientWithResponsesInterface
