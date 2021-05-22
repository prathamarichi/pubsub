<?php

namespace PubSub;

use Google\Cloud\PubSub\PubSubClient;

class Project {

    public $_config = false;
    public $_pubsub = false;

    public function __construct($config) {
        $this->_config = $config;

        $this->_pubsub = new PubSubClient([
            'projectId' => $this->_config->project_id,
            'keyFile' => json_decode(json_encode($this->_config), true)
        ]);
    }

    public function generateProjectName($projectName) {
        $projectName = \strtoupper($projectName);

        return $projectName;
    }

    public function create($projectName) {
        $topicLibrary = new Topic($this->_config);
        $projectName = $this->generateProjectName($projectName);

        $topicName = "notification";
        $topicLibrary->upsert($projectName, $topicName);

        $topicName = "transaction";
        $topicLibrary->upsert($projectName, $topicName);

        $topicName = "export";
        $topicLibrary->upsert($projectName, $topicName);

        $topicName = "general";
        $topicLibrary->upsert($projectName, $topicName);

        return true;
    }

    public function delete($projectName) {
        $topicLibrary = new Topic($this->_config);
        $projectName = $this->generateProjectName($projectName);
        
        //check json file
        $path = __DIR__."/../../storage/pubsub";
        if (!file_exists($path)) mkdir($path, 0777, true);

        $file = $path."/".\strtolower($projectName).".json";
        if (file_exists($file)) $content = json_decode(file_get_contents($file), true);
        else $content = array();

        foreach ($content as $topicName) {
            $topicName = substr($topicName, strlen($projectName)+1);
            $topicLibrary->delete($projectName, $topicName);
        }

        //delete all topic lefts
        $topics = $topicLibrary->list();
        foreach ($topics as $topicName) {
            if (strpos($topicName, $projectName) === 0) $topicLibrary->delete(null, $topicName, true);
        }

        $topics = __DIR__."/../../storage/pubsub/".\strtolower($projectName).".json";
        if (file_exists($topics)) unlink($topics);

        //delete subscriptions
        $subscriberLibrary = new Subscriber($this->_config);
        $subscriberLibrary->clean();

        return true;
    }
}