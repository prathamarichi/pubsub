<?php

namespace PubSub;

use Google\Cloud\PubSub\PubSubClient;

class Client {

    public $_config = false;
    public $_pubsub = false;

    public function __construct($config) {
        $this->_config = $config;

        $this->_pubsub = new PubSubClient([
            'projectId' => $this->_config->project_id,
            'keyFile' => json_decode(json_encode($this->_config), true)
        ]);
    }

    public function getSubscribers($projectName=false, $topicName=false) {
        if ($topicName) {
            $projectName = \strtoupper($projectName);
    
            $server = new Server($this->_config);
            $topic = $server->getTopic($projectName, $topicName);
            $subscribers = $topic->subscriptions();
        } else {
            $subscribers = $this->_pubsub->subscriptions();
        }

        return $subscribers;
    }

    public function getSubscriber($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $subscriber = $this->_pubsub->subscription($subscriptionName);

        return $subscriber;
    }

    public function subscribe($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $server = new Server($this->_config);
        $topic = $server->getCreateTopic($projectName, $topicName);

        $subscription = $topic->subscription($subscriptionName);
        $subscription->create();
    }

    public function unsubscribe($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $server = new Server($this->_config);
        $topic = $server->getTopic($projectName, $topicName);

        $subscription = $topic->subscription($subscriptionName);
        $subscription->delete();
        
        return true;
    }

    public function clean() {
        $subscribers = $this->getSubscribers();
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

    public function pullMessages($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $subscription = $this->_pubsub->subscription($subscriptionName);
        foreach ($subscription->pull() as $message) {
            printf('Message: %s' . PHP_EOL, $message->data());
            // Acknowledge the Pub/Sub message has been received, so it will not be pulled multiple times.
            $subscription->acknowledge($message);
        }
    }
}
