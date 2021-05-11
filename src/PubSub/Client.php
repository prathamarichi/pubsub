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

    public function getSubscribers($projectName) {
        foreach ($this->_pubsub->subscriptions() as $subscription) {
            printf('Subscription: %s' . PHP_EOL, $subscription->name());
        }
    }

    public function getSubscriber($projectName, $subscriptionName) {
        $subscription = $this->_pubsub->subscription($subscriptionName);
        $policy = $subscription->iam()->policy();
        print_r($policy);
    }

    public function createSubscription($projectName, $topicName, $subscriptionName) {
        $projectName = \strtoupper($projectName);
        $subscriptionName = \strtoupper($projectName."-".$topicName."-".$subscriptionName);

        $server = new Server($this->_config);
        $topic = $server->getTopic($projectName, $topicName);

        $subscription = $topic->subscription($subscriptionName);
        $subscription->create();
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
