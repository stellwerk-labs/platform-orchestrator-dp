package orchestrator_graph

import old "fixture-encoder/canyon_graph"

type PlaceholderType = old.PlaceholderType
type PlaceholderSub interface{ Type() PlaceholderType }
type ContextPlaceholder struct{ Key string }

func (*ContextPlaceholder) Type() PlaceholderType { return "context" }

type TfVarPlaceholder struct{ Key string }

func (*TfVarPlaceholder) Type() PlaceholderType { return "var" }

type ResourceCoordinate struct{ Type, Class, Id string }
type ResourcePlaceholder struct {
	Coordinate       *ResourceCoordinate
	Alias            string
	IsShared, IsSelf bool
	Output           []string
}

func (*ResourcePlaceholder) Type() PlaceholderType { return "resource" }

type SelectorPlaceholder struct {
	MatchedCoordinates      []ResourceCoordinate
	Functions, Args, Output []string
}

func (*SelectorPlaceholder) Type() PlaceholderType { return "select" }
