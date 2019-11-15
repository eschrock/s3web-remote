package io.titandata.remote.s3web.server

import io.titandata.remote.RemoteOperation
import io.titandata.remote.RemoteServer
import io.titandata.remote.RemoteServerUtil

/**
 * The S3 web provider is a very simple provider for reading commits created by the S3 provider. It's primary purpose is
 * to make public demo data available without requiring people to have some kind of AWS credentials. It should not be
 * used as a general purpose remote. The URL can be any URL to the S3 bucket, even behind CloudFront, such as:
 *
 *      s3web://demo.titan-data.io/hello-world/postgres
 *
 * The main thing is that it expects to find the same layout as the S3 provider generates, including a "titan" file
 * at the root of the repository that has all the commit metadata.
 */
class S3WebRemoteServer : RemoteServer {

    internal val util = RemoteServerUtil()

    override fun getProvider(): String {
        return "s3web"
    }

    /**
     * S3 Web remotes have only a single property, "url"
     */
    override fun validateRemote(remote: Map<String, Any>): Map<String, Any> {
        util.validateFields(remote, listOf("url"), emptyList())
        return remote
    }

    /**
     * S3 web parameters must always be empty
     */
    override fun validateParameters(parameters: Map<String, Any>): Map<String, Any> {
        util.validateFields(parameters, emptyList(), emptyList())
        return parameters
    }

    override fun getCommit(remote: Map<String, Any>, parameters: Map<String, Any>, commitId: String): Map<String, Any>? {
        throw NotImplementedError()
    }

    override fun listCommits(remote: Map<String, Any>, parameters: Map<String, Any>, tags: List<Pair<String, String?>>): List<Pair<String, Map<String, Any>>> {
        throw NotImplementedError()
    }

    override fun endOperation(operation: RemoteOperation, isSuccessful: Boolean) {
        throw NotImplementedError()
    }

    override fun startOperation(operation: RemoteOperation) {
        throw NotImplementedError()
    }

    override fun syncVolume(operation: RemoteOperation, volumeName: String, volumeDescription: String, volumePath: String, scratchPath: String) {
        throw NotImplementedError()
    }
}
