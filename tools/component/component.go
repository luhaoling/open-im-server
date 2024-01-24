// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/OpenIMSDK/tools/component"
	"github.com/openimsdk/open-im-server/v3/pkg/common/config"

	"gopkg.in/yaml.v3"
)

const (
	// defaultCfgPath is the default path of the configuration file.
	defaultCfgPath = "../../../../../config/config.yaml"
)

var (
	cfgPath = flag.String("c", defaultCfgPath, "Path to the configuration file")
)

func initCfg() error {
	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		return err
	}

	return yaml.Unmarshal(data, &config.Config)
}

func main() {
	flag.Parse()

	if err := initCfg(); err != nil {
		fmt.Printf("Read config failed: %v\n", err)

		return
	}

	checks := []component.CheckFunc{
		//{name: "Mysql", function: checkMysql},
		{Name: "Mongo", Function: component.CheckMongo, Config: config.Config.Mongo},
		{Name: "Minio", Function: component.CheckMinio, Config: config.Config.Object},
		{Name: "Redis", Function: component.CheckRedis, Config: config.Config.Redis},
		{Name: "Zookeeper", Function: component.CheckZookeeper, Config: config.Config.Zookeeper},
		{Name: "Kafka", Function: component.CheckKafka, Config: config.Config.Kafka},
	}

	for i := 0; i < component.MaxRetry; i++ {
		if i != 0 {
			time.Sleep(1 * time.Second)
		}
		fmt.Printf("Checking components Round %v...\n", i+1)

		allSuccess := true
		for _, check := range checks {
			str, err := check.Function(check.Config)
			if err != nil {
				component.ErrorPrint(fmt.Sprintf("Starting %s failed, %v", check.Name, err))
				allSuccess = false
				break
			} else {
				component.SuccessPrint(fmt.Sprintf("%s connected successfully, %s", check.Name, str))
			}
		}

		if allSuccess {
			component.SuccessPrint("All components started successfully!")
			return
		}
	}
	os.Exit(1)
}
