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

    protected function isJson($string) {
        json_decode($string);
        return json_last_error() === JSON_ERROR_NONE;
     }

    public function listProject() {
        $topics = array();
        foreach ($this->_pubsub->topics() as $topic) {
            $topics[] = $topic->name();
        }

        return $topics;
    }

    public function createTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        do {
            try {
                if (!$raw) {
                    $projectName = \strtoupper($projectName);
                    $topicName = $projectName.\strtoupper("-".$topicName);
                }
    
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
                if ($this->isJson($e->getMessage())) {
                    $error = \json_decode($e->getMessage());
                    if (isset($error->error)) {
                        $topic = true;
                        throw new \Exception('Error: '.$e->getMessage());
                    }
                }
            }
        } while (!$topic);

        return $topic;
    }

    public function checkTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        if (!$raw) {
            $projectName = \strtoupper($projectName);
            $topicName = $projectName.\strtoupper("-".$topicName);
        }
        
        //check json file
        $path = __DIR__."/../../storage/pubsub";
        if (!file_exists($path)) mkdir($path, 0777, true);

        $file = $path."/".\strtolower($projectName).".json";
        if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
        else $content = array();

        if (!in_array($topicName, $content)) {
            return false;
        }

        return true;
    }

    public function getTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        if (!$raw) {
            $projectName = \strtoupper($projectName);
            $topicName = $projectName.\strtoupper("-".$topicName);
        }
        $topic = $this->_pubsub->topic($topicName);

        return $topic;
    }

    public function getCreateTopic($projectName, $topicName, $raw=false) {
        $topic = false;

        if ($this->checkTopic($projectName, $topicName, $raw)) {
            $topic = $this->getTopic($projectName, $topicName, $raw);
        } else {
            $topic = $this->createTopic($projectName, $topicName, $raw);
        }

        return $topic;
    }

    public function deleteTopic($projectName, $topicName, $raw=false) {
        $success = false;

        do {
            try {
                if (!$raw) {
                    $projectName = \strtoupper($projectName);
                    $topicName = $projectName.\strtoupper("-".$topicName);
                }
    
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

                $success = true;
            } catch (\Exception $e) {
                if ($this->isJson($e->getMessage())) {
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

    protected function publish($projectName, $topicName, $message, $raw=false) {
        try {
            if (!$raw) {
                $projectName = \strtoupper($projectName);
                $topicName = $projectName.\strtoupper("-".$topicName);
            }
            
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

    public function getTopics($projectName) {
        $projectName = \strtoupper($projectName);

        //delete all project lefts
        $topics = $this->listProject();

        return $topics;
    }

    public function deleteProject($projectName) {
        $projectName = \strtoupper($projectName);
        
        //check json file
        $path = __DIR__."/../../storage/pubsub";
        if (!file_exists($path)) mkdir($path, 0777, true);

        $file = $path."/".\strtolower($projectName).".json";
        if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
        else $content = array();

        foreach ($content as $topicName) {
            $topicName = substr($topicName, strlen($projectName)+1);
            $this->deleteTopic($projectName, $topicName);
        }

        //delete all project lefts
        $topics = $this->listProject();
        foreach ($topics as $topicName) {
            if (strpos($topicName, $projectName) === 0) $this->deleteTopic(false, $topicName, true);
        }

        $topics = __DIR__."/../../storage/pubsub/".\strtolower($projectName).".json";
        if (file_exists($topics)) unlink($topics);

        //delete subscriptions
        $client = new Client($this->_config);
        $client->clean();

        return true;
    }

    public function publishMessage($projectName, $topicName, $message, $expiredAt=false) {
        $projectName = \strtoupper($projectName);
        $this->getCreateTopic($projectName, $topicName);

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