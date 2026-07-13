package logging

import (
	"github.com/stellwerk-labs/golib/hlogger"
	"go.uber.org/zap"
)

func ZapOrgId(o string) zap.Field {
	return zap.String(hlogger.POOrgId, o)
}

func ZapProjectId(a string) zap.Field {
	return zap.String(hlogger.POProjectId, a)
}

func ZapEnvId(e string) zap.Field {
	return zap.String(hlogger.POEnvId, e)
}

func ZapDeploymentId(d string) zap.Field {
	return zap.String(hlogger.PODeployId, d)
}

func ZapRunnerId(r string) zap.Field {
	return zap.String(hlogger.PORunnerId, r)
}
