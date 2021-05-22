<?php

namespace PubSub;

use Google\Cloud\PubSub\PubSubClient;

class Subscriber {

    public $_config = false;
    public $_pubsub = false;

    public function __construct($config) {
        $this->_config = $config;

        $this->_pubsub = new PubSubClient([
            'projectId' => $this->_config->project_id,
            'keyFile' => json_decode(json_encode($this->_config), true)
        ]);
    }

    public function generateSubscriberName($topicName, $subscriberName) {
        $subscriberName = $topicName."-".\strtoupper($subscriberName);

        return $subscriberName;
    }

    public function list($projectName=false, $topicName=false) {
        if ($topicName) {
            $projectLibrary = new Project($this->_config);
            $projectName = $projectLibrary->generateProjectName($projectName);
    
            $topicLibrary = new Topic($this->_config);
            $topicName = $topicLibrary->generateTopicName($projectName, $topicName);
            $topic = $topicLibrary->get($projectName, $topicName);
            
            $subscribers = $topic->subscriptions();
        } else {
            $subscribers = $this->_pubsub->subscriptions();
        }

        return $subscribers;
    }

    public function clean() {
        $subscribers = $this->list();
        foreach ($subscribers as $subscriber) {
            $subscriber = $subscriber->__debugInfo();
            $topicName = \explode("/topics/", $subscriber["topicName"]);
            if (isset($topicName[1]) && $topicName[1] === "_deleted-topic_") {
                $subscription = $this->_pubsub->subscription($subscriber["name"]);
                $subscription->delete();
            }
        }
        
        return true;
    }

    public function create($projectName, $topicName, $subscriberName) {
        $subscription = false;

        do {
            $projectLibrary = new Project($this->_config);
            $projectName = $projectLibrary->generateProjectName($projectName);
    
            $topicLibrary = new Topic($this->_config);
            $topicName = $topicLibrary->generateTopicName($projectName, $topicName);
            $topic = $topicLibrary->upsert($projectName, $topicName);

            $subscriberName = $this->generateSubscriberName($topicName, $subscriberName);
            $subscription = $topic->subscription($subscriberName);
            $subscription->create();
        } while (!$subscription);
    }

    public function get($projectName, $topicName, $subscriberName) {
        $projectLibrary = new Project($this->_config);
        $projectName = $projectLibrary->generateProjectName($projectName);

        $topicLibrary = new Topic($this->_config);
        $topicName = $topicLibrary->generateTopicName($projectName, $topicName);

        $subscriberName = $this->generateSubscriberName($topicName, $subscriberName);
        $subscriber = $this->_pubsub->subscription($subscriberName);

        return $subscriber;
    }

    public function delete($projectName, $topicName, $subscriberName) {
        $projectLibrary = new Project($this->_config);
        $projectName = $projectLibrary->generateProjectName($projectName);

        $topicLibrary = new Topic($this->_config);
        $topicName = $topicLibrary->generateTopicName($projectName, $topicName);
        $topic = $topicLibrary->get($projectName, $topicName);
        
        $subscriberName = $this->generateSubscriberName($topicName, $subscriberName);
        $subscription = $topic->subscription($subscriberName);
        $subscription->delete();
        
        return true;
    }

    public function pull($projectName, $topicName, $subscriberName) {
        $projectLibrary = new Project($this->_config);
        $projectName = $projectLibrary->generateProjectName($projectName);

        $topicLibrary = new Topic($this->_config);
        $topicName = $topicLibrary->generateTopicName($projectName, $topicName);

        $subscriberName = $this->generateSubscriberName($topicName, $subscriberName);
        $subscription = $this->_pubsub->subscription($subscriberName);
        foreach ($subscription->pull() as $message) {
            $subscription->acknowledge($message);
        }
    }
}
