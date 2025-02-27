pipeline {
    agent any

    tools {
        // Define the Maven tool name configured in Jenkins
        maven 'Maven'
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build JAR') {
            steps {
                script {
                    bat 'mvn clean package' // Use 'mvn' or 'gradle' depending on your build tool
                }
            }
        }
    }

    post {
        success {
            archiveArtifacts 'target/*.jar' // Adjust the path based on your project structure
        }
    }
}
