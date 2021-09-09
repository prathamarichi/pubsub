<?php

namespace PubSub;

use Google\Cloud\PubSub\PubSubClient;

class Topic {

    public $_config = false;
    public $_pubsub = false;

    public function __construct($config) {
        $this->_config = $config;

        $this->_pubsub = new PubSubClient([
            'projectId' => $this->_config->project_id,
            'keyFile' => json_decode(json_encode($this->_config), true)
        ]);
    }

    public function generateTopicName($projectName, $topicName) {
        $topicName = $projectName."-".$topicName;

        return $topicName;
    }

    public function exist($topicName) {
        //check json file
        $path = realpath(".")."/public/storage/pubsub";
        if (!file_exists($path)) mkdir($path, 0777, true);

        $file = $path."/topic.json";
        if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
        else $content = array();

        if (!in_array($topicName, $content)) {
            return false;
        }

        return true;
    }

    public function upsert($topicName) {
        $topic = false;

        if ($this->exist($topicName)) {
            $topic = $this->get($topicName);
        } else {
            $topic = $this->create($topicName);
        }

        return $topic;
    }

    public function list() {
        $topics = array();
        foreach ($this->_pubsub->topics() as $topic) {
            $topics[] = $topic->name();
        }

        return $topics;
    }

    public function create($topicName) {
        do {
            try {
                //add to json file
                $path = realpath(".")."/public/storage/pubsub";
                if (!file_exists($path)) mkdir($path, 0777, true);
    
                $file = $path."/topic.json";
                if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
                else $content = array();
    
                if (!in_array($topicName, $content)) {
                    $topic = $this->_pubsub->createTopic($topicName);
                    
                    $content[] = $topicName;
                    $content = json_encode($content);
                    file_put_contents($file, $content);
                }
            } catch (\Exception $e) {
                if (General::isJson($e->getMessage())) {
                    $error = \json_decode($e->getMessage());
                    if (isset($error->error)) {
                        $content[] = $topicName;
                        $content = json_encode($content);
                        file_put_contents($file, $content);

                        $topic = true;
                        return false;
                    }
                }
            }
        } while (!$topic);

        return $topic;
    }

    public function get($topicName) {
        $topic = $this->_pubsub->topic($topicName);

        return $topic;
    }

    public function delete($topicName) {
        $success = false;
        do {
            try {
                $topic = $this->_pubsub->topic($topicName);
                $topic->delete();
        
                //remove from json file
                $path = realpath(".")."/public/storage/pubsub";
                if (!file_exists($path)) mkdir($path, 0777, true);
    
                $file = $path."/topic.json";
                if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
                else $content = array();
    
                if (in_array($topicName, $content)) {
                    if (($pos = array_search($topicName, $content)) !== false) unset($content[$pos]);
    
                    $content = json_encode($content);
                    file_put_contents($file, $content);
                }

                $success = true;
            } catch (\Exception $e) {
                if (General::isJson($e->getMessage())) {
                    $error = \json_decode($e->getMessage());
                    if (isset($error->error)) {
                        $success = true;
                        throw new \Exception('Error: '.$e->getMessage());
                    }
                }
            }
        } while (!$success);

        return true;
    }

    public function publish($topicName, $message, $expiredAt=false) {
        $this->upsert($topicName);

        if ($expiredAt) $expiredAt = \strtotime("+2 hours");
        else $expiredAt = \strtotime("+2 hours");

        $data = array(
            "content" => $message,
            "expired_at" => $expiredAt
        );
        $message = json_encode($data);
        $topic = $this->_pubsub->topic($topicName);
        $message = $topic->publish(['data' => $message]);

        return $message;
    }
}