package io.titandata.remote.s3web.server

import io.kotlintest.TestCase
import io.kotlintest.TestCaseOrder
import io.kotlintest.TestResult
import io.kotlintest.shouldBe
import io.kotlintest.shouldThrow
import io.kotlintest.specs.StringSpec
import io.mockk.MockKAnnotations
import io.mockk.clearAllMocks
import io.mockk.impl.annotations.SpyK
import kotlin.IllegalArgumentException

class S3WebRemoteServerTest : StringSpec() {

    @SpyK
    var client = S3WebRemoteServer()

    override fun beforeTest(testCase: TestCase) {
        return MockKAnnotations.init(this)
    }

    override fun afterTest(testCase: TestCase, result: TestResult) {
        clearAllMocks()
    }

    override fun testCaseOrder() = TestCaseOrder.Random

    init {
        "get provider returns s3web" {
            client.getProvider() shouldBe "s3web"
        }

        "validate remote succeeds with only required properties" {
            val result = client.validateRemote(mapOf("url" to "url"))
            result["url"] shouldBe "url"
        }

        "validate remote fails with missing required property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(emptyMap())
            }
        }

        "validate remote fails with invalid property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(mapOf("url" to "url", "foo" to "bar"))
            }
        }

        "validate parameters succeeds with empty properties" {
            val result = client.validateParameters(emptyMap())
            result.size shouldBe 0
        }

        "validate parameters fails with invalid property" {
            shouldThrow<IllegalArgumentException> {
                client.validateRemote(mapOf("foo" to "bar"))
            }
        }
    }
}
