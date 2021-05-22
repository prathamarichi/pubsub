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

    public function list($projectName=false, $topicName=false) {
        if ($topicName) {
            $projectName = \strtoupper($projectName);
    
            $topicLibrary = new Topic($this->_config);
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

    public function create($projectName, $topicName, $subscriptionName) {
        $subscription = false;

        do {
            $projectName = \strtoupper($projectName);
            $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);
    
            $topicLibrary = new Topic($this->_config);
            $topic = $topicLibrary->upsert($projectName, $topicName);
    
            $subscription = $topic->subscription($subscriptionName);
            $subscription->create();
        } while (!$subscription);
    }

    public function get($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $subscriber = $this->_pubsub->subscription($subscriptionName);

        return $subscriber;
    }

    public function delete($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $topicLibrary = new Topic($this->_config);
        $topic = $topicLibrary->get($projectName, $topicName);

        $subscription = $topic->subscription($subscriptionName);
        $subscription->delete();
        
        return true;
    }

    public function pull($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $subscription = $this->_pubsub->subscription($subscriptionName);
        foreach ($subscription->pull() as $message) {
            $subscription->acknowledge($message);
        }
    }
}
