package model

import (
	"testing"

	"github.com/google/uuid"
	cp "github.com/stellwerk-labs/platform-orchestrator-cp/shared/v3/genclient"
	"github.com/stretchr/testify/require"
)

func TestAdmissionPredecessorCannotQualifyLegacyOrMalformedNativeIntent(t *testing.T) {
	for _, mode := range []DeploymentMode{DeploymentModeDeploy, DeploymentModeDeployPlan, DeploymentModeRollbackPlan} {
		receipt := DeploymentAdmissionReceipt{Mode: mode, NativeTarget: testNativeTarget(), NativeAdmissionWatermark: &cp.NativeAdmissionWatermark{AdmissionId: uuid.New(), Epoch: 3}}
		require.NoError(t, validateAdmissionNativeIntent(&receipt))
		receipt.NativeTarget = nil
		require.Error(t, validateAdmissionNativeIntent(&receipt), "a predecessor cannot authorize a legacy target")
		receipt.NativeTarget = testNativeTarget()
		receipt.NativeAdmissionWatermark.Epoch = 0
		require.Error(t, validateAdmissionNativeIntent(&receipt))
		receipt.NativeAdmissionWatermark.Epoch = 1
		receipt.NativeAdmissionWatermark.AdmissionId = uuid.Nil
		require.Error(t, validateAdmissionNativeIntent(&receipt))
		receipt.NativeAdmissionWatermark = nil
		receipt.NativeTarget.ToolsDigest = "invalid"
		require.Error(t, validateAdmissionNativeIntent(&receipt))
	}
	for _, mode := range []DeploymentMode{DeploymentModeDeployPlan, DeploymentModeRollbackPlan} {
		require.Error(t, validateAdmissionNativeIntent(&DeploymentAdmissionReceipt{Mode: mode}), "Plan receipts require Native authority")
	}
	require.NoError(t, validateAdmissionNativeIntent(&DeploymentAdmissionReceipt{Mode: DeploymentModeDeploy}))
}
