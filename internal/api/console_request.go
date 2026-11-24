package api

import (
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v4"
)

const consoleJSONLimit = 1 << 20

func (s *Server) decodeConsoleJSON(c echo.Context, target any) error {
	if origin := c.Request().Header.Get("Origin"); origin != "" {
		u, err := url.Parse(s.UiHostUrl)
		if err != nil || u.Host == "" || origin != u.Scheme+"://"+u.Host {
			return echo.NewHTTPError(http.StatusForbidden, "origin refused")
		}
	}
	media, _, err := mime.ParseMediaType(c.Request().Header.Get("Content-Type"))
	if err != nil || media != observationJSONContentType {
		return echo.NewHTTPError(http.StatusUnsupportedMediaType, "JSON required")
	}
	d := json.NewDecoder(io.LimitReader(c.Request().Body, consoleJSONLimit+1))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid request")
	}
	return nil
}
