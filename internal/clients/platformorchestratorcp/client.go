package platformorchestratorcp

//go:generate go tool mockgen -destination mocks/client_mock.go -package mockplatformorchestratorcp github.com/stellwerk-labs/platform-orchestrator-cp/shared/genclient ClientWithResponsesInterface
