package converge

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConverge(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MaxScale config convergence Suite")
}
