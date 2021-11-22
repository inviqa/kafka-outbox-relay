pipeline {
    agent { label "my127ws" }
    environment {
        COMPOSE_DOCKER_CLI_BUILD = 1
        DOCKER_BUILDKIT = 1
        MY127WS_KEY = credentials('kafka-outbox-relay-my127ws-key')
        MY127WS_ENV = "pipeline"
    }
    triggers { cron(env.BRANCH_NAME == '' ? 'H H(0-6) * * *' : '') }
    stages {
        stage('Setup') {
            steps { sh 'ws docker build-context' }
        }
        stage('Build') {
            steps { sh 'ws install' }
        }
        stage('Test')  {
            parallel {
                stage('go mod check') { steps { sh 'ws go docker mod check' } }
                stage('go fmt') { steps { sh 'ws go docker fmt check' } }
                stage('go test') { steps { sh 'ws go docker test' } }
                stage('go vet') { steps { sh 'ws go docker vet' } }
                stage('go gocyclo') { steps { sh 'ws go docker gocyclo' } }
                stage('go ineffassign') { steps { sh 'ws go docker ineffassign' } }
                stage('go gosec') { steps { sh 'ws go docker gosec' } }
                stage('helm kubeval qa') { steps { sh 'ws helm kubeval qa' } }
            }
        }
        stage('Integration Tests') {
            steps { sh 'ws go test integration docker all' }
        }
        stage('Build for production') {
            steps {
                sh 'ws build-prod'
            }
        }
        stage('Publish') {
            environment {
                DOCKER_REGISTRY_CREDS = credentials('inviqa-quay-registry-credentials')
            }
            when {
                not { triggeredBy 'TimerTrigger' }
                anyOf {
                    branch 'master'
                }
            }
            steps {
                milestone(20)
                sh 'ws app publish'
            }
        }
    }
    post {
        always {
            sh 'ws destroy'
            cleanWs()
        }
    }
}
