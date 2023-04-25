// generator is a package that exposes functionality for taking a set of Nomad
// Allocations and a template to produce a Vector configuration.
package generator

type Vector struct {
	targetConfigDir string
	template        string
}
