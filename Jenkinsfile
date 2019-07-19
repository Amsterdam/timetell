#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block();
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t;
    }
    finally {
        if (tearDown) {
            tearDown();
        }
    }
}


node {

    stage("Checkout") {
        checkout scm
    }

    stage('Test') {
        tryStep "test", {
            sh "docker-compose -p timetell -f src/.jenkins/test/docker-compose.yml build && " +
               "docker-compose -p timetell -f src/.jenkins/test/docker-compose.yml run -u root --rm test"
        }, {
            sh "docker-compose -p timetell -f src/.jenkins/test/docker-compose.yml down"
        }
    }

        stage("Build dockers") {
        tryStep "build", {
            def image = docker.build("repo.data.amsterdam.nl/datapunt/timetell:${env.BUILD_NUMBER}", "src")
            image.push()
            image.push("acceptance")

        }
    }
}

String BRANCH = "${env.BRANCH_NAME}"

if (BRANCH == "master") {

    node {
        stage('Push acceptance image') {
            tryStep "image tagging", {
                def image = docker.image("repo.data.amsterdam.nl/datapunt/timetell:${env.BUILD_NUMBER}")
                image.pull()
                image.push("acceptance")
            }
        }
    }

    node {
        stage("Deploy to ACC") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                parameters: [
                    [$class: 'StringParameterValue', name: 'INVENTORY', value: 'acceptance'],
                    [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy-timetell.yml'],
                ]
            }
        }
    }


    stage('Waiting for approval') {
        slackSend channel: '#ci-channel', color: 'warning', message: 'Meldingen is waiting for Production Release - please confirm'
        input "Deploy to Production?"
    }

    node {
        stage('Push production image') {
            tryStep "image tagging", {
                def api = docker.image("repo.data.amsterdam.nl/datapunt/timetell:${env.BUILD_NUMBER}")
                api.pull()
                api.push("production")
                api.push("latest")
            }
        }
    }

    node {
        stage("Deploy") {
            tryStep "deployment", {
                build job: 'Subtask_Openstack_Playbook',
                parameters: [
                        [$class: 'StringParameterValue', name: 'INVENTORY', value: 'production'],
                        [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy-timetell.yml'],
                ]
            }
        }
    }
}