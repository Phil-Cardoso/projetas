pipeline {
    agent any
    options {
        buildDiscarder(logRotator(numToKeepStr: '5'))
    }
    stages {
        stage('Scan') {
            steps {
                withSonarQubeEnv(installationName: 'sq1', credentialsId: 'sq1') {
                    sh './gradlew sonarqube'
                }
            }
        }
    }
}