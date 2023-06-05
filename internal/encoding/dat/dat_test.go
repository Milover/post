package dat

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type readTest struct {
	Name   string
	Error  error
	Input  string
	Output [][]string
}

var readTests = []readTest{
	{
		Name:  "good-empty",
		Error: nil,
		Input: `
`,
		Output: nil,
	},
	{
		Name:  "good-empty-multiline",
		Error: nil,
		Input: `


`,
		Output: nil,
	},
	{
		Name:  "good-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
`,
		Output: nil,
	},
	{
		Name:  "good-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
`,
		Output: nil,
	},
	{
		Name:  "good-basic",
		Error: nil,
		Input: `
t	x y z
`,
		Output: [][]string{{"t", "x", "y", "z"}},
	},
	{
		Name:  "good-basic-multiline",
		Error: nil,
		Input: `
t	x y z
t	x y z
t	x y z
`,
		Output: [][]string{
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
		},
	},
	{
		Name:  "good-basic-w-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
t	x y z
`,
		Output: [][]string{{"t", "x", "y", "z"}},
	},
	{
		Name:  "good-basic-multiline-w-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
t	x y z
t	x y z
t	x y z
`,
		Output: [][]string{
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
		},
	},
	{
		Name:  "good-basic-multiline-w-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
t	x y z
t	x y z
t	x y z
`,
		Output: [][]string{
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
		},
	},
	{
		Name:  "good-basic-multiline-w-comments-variable-spaces",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
	t	    x 	y	    z	    
  	t       x   y		z		
    t		x   y 	    z       
`,
		Output: [][]string{
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
		},
	},
	{
		Name:  "good-basic-multiline-w-comments-variable-spaces-empty-multiline",
		Error: nil,
		Input: `
# A comment which should be skipped

# Another comment which should be skipped
	t	    x 	y	    z	    

  	t       x   y		z		


    t		x   y 	    z       
`,
		Output: [][]string{
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
			{"t", "x", "y", "z"},
		},
	},
	{
		Name:  "good-vector",
		Error: nil,
		Input: `
t	(x y z) (x y z) (x y z)
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-vector-multiline",
		Error: nil,
		Input: `
t	(x y z) (x y z) (x y z)
t	(x y z) (x y z) (x y z)
t	(x y z) (x y z) (x y z)
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-vector-w-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
t	(x y z) (x y z) (x y z)
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-vector-w-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
t	(x y z) (x y z) (x y z)
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-vector-multiline-w-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
t	(x y z) (x y z) (x y z)
t	(x y z) (x y z) (x y z)
t	(x y z) (x y z) (x y z)
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-tensor",
		Error: nil,
		Input: `
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-tensor-multiline",
		Error: nil,
		Input: `
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-tensor-w-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-tensor-w-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-tensor-multiline-w-comments",
		Error: nil,
		Input: `
# A comment which should be skipped
# Another comment which should be skipped
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
t	((x y z) (x y z) (x y z)) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-mixed",
		Error: nil,
		Input: `
t	x (x y z) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-mixed-multiline",
		Error: nil,
		Input: `
t	x (x y z) ((x y z) (x y z) (x y z))
t	x (x y z) ((x y z) (x y z) (x y z))
t	x (x y z) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-mixed-w-comment",
		Error: nil,
		Input: `
# A comment which should be skipped
t	x (x y z) ((x y z) (x y z) (x y z))
`,
		Output: [][]string{
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
	{
		Name:  "good-mixed-multiline-w-comments-variable-spaces-empty-multiline",
		Error: nil,
		Input: `
# A comment which should be skipped

# Another comment which should be skipped
		t	x	(x y z)	    ((x y z) (x y z) (x y z))	    

        t	x   (x y z)     ((x y z) (x y z) (x y z))       


	    t	x	(x y z)		((x y z) (x y z) (x y z))		
`,
		Output: [][]string{
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
			{"t", "x", "x", "y", "z", "x", "y", "z", "x", "y", "z", "x", "y", "z"},
		},
	},
}

func TestReadAll(t *testing.T) {
	for _, tt := range readTests {
		t.Run(tt.Name, func(t *testing.T) {
			assert := assert.New(t)
			r := NewReader(strings.NewReader(tt.Input))

			out, err := r.ReadAll()

			assert.Equal(tt.Error, err)
			if tt.Error != nil {
				assert.Nil(out)
			} else {
				assert.Equal(tt.Output, out)
			}
		})
	}
}
