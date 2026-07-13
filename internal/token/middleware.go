package token

import (
	"net/http"
	"reflect"

	"github.com/labstack/echo/v4"
	strictecho "github.com/oapi-codegen/runtime/strictmiddleware/echo"
	"github.com/stellwerk-labs/platform-orchestrator-dp/internal/ref"
)

func StrictEncryptionMiddleware() func(f strictecho.StrictEchoHandlerFunc, operationID string) strictecho.StrictEchoHandlerFunc {
	return func(f strictecho.StrictEchoHandlerFunc, operationID string) strictecho.StrictEchoHandlerFunc {
		return func(ctx echo.Context, request interface{}) (interface{}, error) {
			requestValue := reflect.ValueOf(request)
			newRequest := reflect.New(requestValue.Type()).Elem()
			newRequest.Set(requestValue)

			if p := reflect.ValueOf(&newRequest).Elem().FieldByName("Params"); p.IsValid() {
				if pp := p.FieldByName("Page"); pp.IsValid() {
					if pp.Elem().IsValid() {
						pageToken, err := decrypt(pp.String())
						if err != nil {
							return nil, echo.NewHTTPError(http.StatusBadRequest, "Invalid page token")
						}
						pp.Set(reflect.ValueOf(ref.Ref(pageToken)))
					}
				}
			}

			response, err := f(ctx, newRequest.Interface())

			if response != nil {
				responseValue := reflect.ValueOf(response)
				newResponse := reflect.New(responseValue.Type()).Elem()
				newResponse.Set(responseValue)

				if p := reflect.ValueOf(&newResponse).Elem().FieldByName("NextPageToken"); p.IsValid() {
					if p.Elem().IsValid() {
						pageToken, err := encrypt(p.String())
						if err != nil {
							return nil, echo.NewHTTPError(http.StatusInternalServerError, "Failed to encrypt page token")
						}
						p.Set(reflect.ValueOf(ref.Ref(pageToken)))
					}
				}
				return newResponse.Interface(), err
			}
			return response, err
		}
	}
}
