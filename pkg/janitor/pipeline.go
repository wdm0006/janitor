package janitor

import "context"

// Transform is a mutation or validation applied to a Frame.
// Streaming versions will operate on chunks; this is a simple placeholder.
type Transform interface {
	Name() string
	Apply(ctx context.Context, f *Frame) (*Frame, error)
}

// Pipeline composes a sequence of Transforms.
type Pipeline struct {
	steps []Transform
}

func NewPipeline() *Pipeline { return &Pipeline{} }

func (p *Pipeline) Add(t Transform) *Pipeline {
	p.steps = append(p.steps, t)
	return p
}

func (p *Pipeline) Run(ctx context.Context, f *Frame) (*Frame, error) {
	var err error
	cur := f
	for _, t := range p.steps {
		cur, err = t.Apply(ctx, cur)
		if err != nil {
			return nil, err
		}
	}
	return cur, nil
}
