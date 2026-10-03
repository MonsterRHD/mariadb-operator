package maxscaleconfigchange

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestConfigChange(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "MaxScale configuration change Suite")
}
