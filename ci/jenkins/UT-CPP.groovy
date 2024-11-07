@Library('jenkins-shared-library@v0.67.0') _

def pod = libraryResource 'io/milvus/pod/tekton-4am.yaml'
def milvus_helm_chart_version = '4.2.8'

pipeline {
    options {
        skipDefaultCheckout true
        parallelsAlwaysFailFast()
        buildDiscarder logRotator(artifactDaysToKeepStr: '30')
        preserveStashes(buildCount: 5)
        disableConcurrentBuilds(abortPrevious: true)
        timeout(time: 6, unit: 'HOURS')
        throttleJobProperty(
            categories: ['cpp-unit-test'],
            throttleEnabled: true,
            throttleOption: 'category'

        )
    }
    agent {
        kubernetes {
            cloud '4am'
            yaml pod
        }
    }
    stages {
        stage('meta') {
            steps {
                container('jnlp') {
                    script {
                        isPr = env.CHANGE_ID != null
                        gitMode = isPr ? 'merge' : 'fetch'
                        gitBaseRef = isPr ? "$env.CHANGE_TARGET" : "$env.BRANCH_NAME"
                    }
                }
            }
        }
        stage('build & test') {
            steps {
                container('tkn') {
                    script {
                        def job_name = tekton.cpp_ut arch: 'amd64',
                                              isPr: isPr,
                                              gitMode: gitMode ,
                                              gitBaseRef: gitBaseRef,
                                              pullRequestNumber: "$env.CHANGE_ID",
                                              make_cmd: "make clean && make USE_ASAN=ON build-cpp-with-coverage",
                                              test_entrypoint: "./scripts/run_cpp_codecov.sh",
                                              codecov_report_name: "cpp-unit-test",
                                              codecov_files: "./lcov_output.info",
                                              tekton_pipeline_timeout: '3h'
                    }
                }
            }
            post {
                always {
                    container('tkn') {
                        script {
                            tekton.sure_stop()
                        }
                    }
                }
            }
        }
    }
}