package dat

import (
	"strings"
	"testing"
)

type readTest struct {
	Name  string
	Error error
	Input string
}

var readTests = []readTest{
	{
		Name:  "good-y-plus",
		Error: nil,
		Input: `
# y+ ()             
# Time              	patch               	min                 	max                 	average             
0.00277777777778    	wall.bottom	4.676823261693e-01	5.732051847173e-01	5.235692408566e-01
0.00555555555556    	wall.bottom	4.710375411354e-01	5.783197070958e-01	5.277825566613e-01
0.00833333333334    	wall.bottom	4.750251681566e-01	5.830207560139e-01	5.318746059146e-01
`,
	},
	{
		Name:  "good-wall-shear-stress",
		Error: nil,
		Input: `
# Wall shear stress 
# Time              	patch               	min                 	max                 
0.00277777777778    	wall.bottom	(-4.169687983904e-03 -1.668569404092e-04 -9.453757662062e-06)	(-2.775643611398e-03 3.043129201494e-04 7.311402783276e-06)
0.00555555555556    	wall.bottom	(-4.244291851466e-03 -1.711696591792e-04 -9.853797992001e-06)	(-2.814548526212e-03 3.389678056119e-04 8.195634027997e-06)
0.00833333333334    	wall.bottom	(-4.313760221413e-03 -2.164288631183e-04 -9.675137602216e-06)	(-2.861768939271e-03 3.794252678965e-04 8.688882690234e-06)
`,
	},
	{
		Name:  "good-probes",
		Error: nil,
		Input: `
# Probe 0 (0.015139702765 0.0075698513825 0) at patch wall.bottom with a distance of 0 m to the original point (0.015139702765 0.0075698513825 0)
#             Probe                   0
#              Time
  0.000555555555556       0.52096565996
   0.00111111111111      0.517831692712
   0.00166666666667      0.514593344581
`,
	},
	{
		Name:  "good-forces",
		Error: nil,
		Input: `
# Force             
# CofR              : (0.000000000000e+00 0.000000000000e+00 0.000000000000e+00)
#
# Time              	(total_x total_y total_z)	(pressure_x pressure_y pressure_z)	(viscous_x viscous_y viscous_z)
2.77777777778e-05   	(1.569780581714e-03 1.154940245907e-06 1.126044702712e-03)	(0.000000000000e+00 0.000000000000e+00 1.126044548122e-03)	(1.569780581714e-03 1.154940245907e-06 1.545900254555e-10)
5.55555555556e-05   	(1.570035715627e-03 1.154461320288e-06 1.457697491097e-03)	(0.000000000000e+00 0.000000000000e+00 1.457697300450e-03)	(1.570035715627e-03 1.154461320288e-06 1.906464182878e-10)
8.33333333334e-05   	(1.570293111532e-03 1.153978799148e-06 1.342258162440e-03)	(0.000000000000e+00 0.000000000000e+00 1.342257945696e-03)	(1.570293111532e-03 1.153978799148e-06 2.167441823708e-10)
`,
	},
}

func TestReader(t *testing.T) {
	for _, tt := range readTests {
		t.Run(tt.Name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.Input))
			_, err := r.ReadAll()
			if err != tt.Error {
				t.Fatal(err)
			}
			/* TODO: write some actual tests
			for _, record := range records {
				for _, r := range record {
					fmt.Printf("%q", r)
				}
				fmt.Println()
			}
			*/
		})
	}
}
