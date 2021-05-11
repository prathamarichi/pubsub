<?php

namespace PubSub;

use Google\Cloud\PubSub\PubSubClient;

class Server {

    public $_config = false;
    public $_pubsub = false;

    public function __construct($config) {
        $this->_config = $config;

        $this->_pubsub = new PubSubClient([
            'projectId' => $this->_config->project_id,
            'keyFile' => json_decode(json_encode($this->_config), true)
        ]);
    }

    public function listProject() {
        $topics = array();
        foreach ($this->_pubsub->topics() as $topic) {
            $topics[] = $topic->name();
        }

        return $topics;
    }

    protected function createTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        try {
            if (!$raw) $topicName = $projectName."-".$topicName;

            //add to json file
            $path = __DIR__."/../../storage/pubsub";
            if (!file_exists($path)) mkdir($path, 0777, true);

            $file = $path."/".\strtolower($projectName).".json";
            if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
            else $content = array();

            if (!in_array($topicName, $content)) {
                $content[] = $topicName;
                $topic = $this->_pubsub->createTopic($topicName);

                $content = json_encode($content);
                file_put_contents($file, $content);
            }
        } catch (\Exception $e) {
            //do nothing
        }

        return $topic;
    }

    public function getTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        if (!$raw) $topicName = $projectName."-".$topicName;
        $topic = $this->_pubsub->topic($topicName);

        return $topic;
    }

    protected function deleteTopic($projectName, $topicName, $raw=false) {
        try {
            if (!$raw) $topicName = $projectName."-".$topicName;
            $topic = $this->_pubsub->topic($topicName);
            $topic->delete();
    
            //remove from json file
            $path = __DIR__."/../../storage/pubsub";
            if (!file_exists($path)) mkdir($path, 0777, true);

            $file = $path."/".\strtolower($projectName).".json";
            if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
            else $content = array();

            if (in_array($topicName, $content)) {
                if (($pos = array_search($topicName, $content)) !== false) unset($content[$pos]);

                $content = json_encode($content);
                file_put_contents($file, $content);
            }
        } catch (\Exception $e) {
            //do nothing
        }

        return true;
    }

    protected function publish($projectName, $topicName, $message, $raw=false) {
        try {
            if (!$raw) $topicName = $projectName."-".$topicName;
            $topic = $this->_pubsub->topic($topicName);
            $message = $topic->publish(['data' => $message]);
        } catch (\Exception $e) {
            throw new \Exception('Error: '.$e->getMessage());
        }

        return $message;
    }

    public function createProject($projectName) {
        $projectName = \strtoupper($projectName);

        $topicName = "notification";
        $this->createTopic($projectName, $topicName);

        $topicName = "transaction";
        $this->createTopic($projectName, $topicName);

        $topicName = "export";
        $this->createTopic($projectName, $topicName);

        $topicName = "general";
        $this->createTopic($projectName, $topicName);

        return true;
    }

    public function deleteProject($projectName) {
        $projectName = \strtoupper($projectName);

        $topicName = "notification";
        $this->deleteTopic($projectName, $topicName);

        $topicName = "transaction";
        $this->deleteTopic($projectName, $topicName);

        $topicName = "export";
        $this->deleteTopic($projectName, $topicName);

        $topicName = "general";
        $this->deleteTopic($projectName, $topicName);

        //delete all project lefts
        $topics = $this->listProject();
        foreach ($topics as $topicName) {
            if (strpos($topicName, $projectName) === 0) $this->deleteTopic(false, $topicName, true);
        }

        $topics = __DIR__."/../../storage/pubsub/".\strtolower($projectName).".json";
        if (file_exists($topics)) unlink($topics);

        return true;
    }

    public function publishMessage($projectName, $topicName, $message, $expiredAt=false) {
        $projectName = \strtoupper($projectName);

        if ($expiredAt) $expiredAt = \strtotime("+4 hours");
        else $expiredAt = \strtotime("+4 hours");

        $data = array(
            "content" => $message,
            "expired_at" => $expiredAt
        );
        $message = $this->publish($projectName, $topicName, json_encode($data));

        return $message;
    }
}