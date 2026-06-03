param environmentName string = 'prod'
param primaryLocation string = 'eastus'
param githubImage string = 'ghcr.io/niravrp/distributed-sharded-ledger:latest'

var logAnalyticsWorkspaceName = 'log-distributed-ledger-${environmentName}'
var eventHubNamespaceName = 'evhns-ledger-${uniqueString(resourceGroup().id)}'

// ==========================================
// 1. CENTRALIZED PIPELINES (East US)
// ==========================================

// Log Analytics Workspace (Free 5GB Tier)
resource logAnalyticsWorkspace 'Microsoft.OperationalInsights/workspaces@2022-10-01' = {
  name: logAnalyticsWorkspaceName
  location: primaryLocation
  properties: {
    sku: { name: 'PerGB2018' }
    retentionInDays: 30
  }
}

// Managed Event Hubs Kafka Namespace (Standard Tier)
resource eventHubNamespace 'Microsoft.EventHub/namespaces@2024-01-01' = {
  name: eventHubNamespaceName
  location: primaryLocation
  sku: {
    name: 'Standard'
    tier: 'Standard'
    capacity: 1
  }
  properties: {
    kafkaEnabled: true
  }
}

// Kafka Topic for Ledger Transaction Streams
resource transactionEventHub 'Microsoft.EventHub/namespaces/eventhubs@2024-01-01' = {
  parent: eventHubNamespace
  name: 'ledger-transactions'
  properties: {
    messageRetentionInDays: 1
    partitionCount: 4
  }
}

// Reference connection rule to securely extract connection string keys
resource eventHubAuthRule 'Microsoft.EventHub/namespaces/authorizationRules@2024-01-01' existing = {
  parent: eventHubNamespace
  name: 'RootManageSharedAccessKey'
}

// ==========================================
// 2. THE THREE REGIONAL CONTAINER NETWORKS
// ==========================================

resource caeEastUs 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'cae-ledger-eastus'
  location: 'eastus'
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsWorkspace.properties.customerId
        sharedKey: logAnalyticsWorkspace.listKeys().primarySharedKey
      }
    }
  }
}

resource caeWestUs 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'cae-ledger-westus'
  location: 'westus'
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsWorkspace.properties.customerId
        sharedKey: logAnalyticsWorkspace.listKeys().primarySharedKey
      }
    }
  }
}

resource caeNorthEurope 'Microsoft.App/managedEnvironments@2024-03-01' = {
  name: 'cae-ledger-northeurope'
  location: 'northeurope'
  properties: {
    appLogsConfiguration: {
      destination: 'log-analytics'
      logAnalyticsConfiguration: {
        customerId: logAnalyticsWorkspace.properties.customerId
        sharedKey: logAnalyticsWorkspace.listKeys().primarySharedKey
      }
    }
  }
}

// ==========================================
// 3. STATEFUL STORAGE SHARDS (Distributed Globally)
// ==========================================

resource shardEastUs 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'shard-eastus'
  location: 'eastus'
  properties: {
    managedEnvironmentId: caeEastUs.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5001, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'shard-eastus'
          image: githubImage
          command: [ 'node', 'dist/storageServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: [
            { name: 'PORT', value: '5001' }
            { name: 'NODE_NAME', value: 'shard-eastus' }
            { name: 'STORAGE_DIR', value: '/app/storage/eastus' }
          ]
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

resource shardWestUs 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'shard-westus'
  location: 'westus'
  properties: {
    managedEnvironmentId: caeWestUs.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5001, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'shard-westus'
          image: githubImage
          command: [ 'node', 'dist/storageServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: [
            { name: 'PORT', value: '5001' }
            { name: 'NODE_NAME', value: 'shard-westus' }
            { name: 'STORAGE_DIR', value: '/app/storage/westus' }
          ]
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

resource shardNorthEurope 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'shard-northeurope'
  location: 'northeurope'
  properties: {
    managedEnvironmentId: caeNorthEurope.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5001, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'shard-northeurope'
          image: githubImage
          command: [ 'node', 'dist/storageServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: [
            { name: 'PORT', value: '5001' }
            { name: 'NODE_NAME', value: 'shard-northeurope' }
            { name: 'STORAGE_DIR', value: '/app/storage/northeurope' }
          ]
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

// ==========================================
// 4. STATELESS INGRESS ROUTERS (Distributed Globally)
// ==========================================

// Global environment list matrix to inject into routers
var shardEnvVariables = [
  { name: 'PORT', value: '5000' }
  { name: 'KAFKA_BROKER', value: '${eventHubNamespaceName}.servicebus.windows.net:9093' }
  { name: 'KAFKA_CONNECTION_STRING', value: eventHubAuthRule.listKeys().primaryConnectionString }
  { name: 'SHARD_EASTUS_URL', value: 'https://${shardEastUs.properties.configuration.ingress.fqdn}' }
  { name: 'SHARD_WESTUS_URL', value: 'https://${shardWestUs.properties.configuration.ingress.fqdn}' }
  { name: 'SHARD_NORTHEUROPE_URL', value: 'https://${shardNorthEurope.properties.configuration.ingress.fqdn}' }
]

resource apiRouterEastUs 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'api-router-eastus'
  location: 'eastus'
  properties: {
    managedEnvironmentId: caeEastUs.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5000, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'api-router'
          image: githubImage
          command: [ 'node', 'dist/routerServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: shardEnvVariables
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

resource apiRouterWestUs 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'api-router-westus'
  location: 'westus'
  properties: {
    managedEnvironmentId: caeWestUs.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5000, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'api-router'
          image: githubImage
          command: [ 'node', 'dist/routerServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: shardEnvVariables
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

resource apiRouterNorthEurope 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'api-router-northeurope'
  location: 'northeurope'
  properties: {
    managedEnvironmentId: caeNorthEurope.id
    configuration: {
      activeRevisionsMode: 'Single'
      ingress: { external: true, targetPort: 5000, transport: 'auto' }
    }
    template: {
      containers: [
        {
          name: 'api-router'
          image: githubImage
          command: [ 'node', 'dist/routerServer.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: shardEnvVariables
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 1 }
    }
  }
}

// ==========================================
// 5. THE CENTRAL BACKGROUND WORKER (East US)
// ==========================================
resource ledgerWorker 'Microsoft.App/containerApps@2024-03-01' = {
  name: 'ledger-worker'
  location: primaryLocation
  properties: {
    managedEnvironmentId: caeEastUs.id
    configuration: { activeRevisionsMode: 'Single' }
    template: {
      containers: [
        {
          name: 'ledger-worker'
          image: githubImage
          command: [ 'node', 'dist/ledgerWorker.js' ]
          resources: { cpu: json('0.25'), memory: '0.5Gi' }
          env: shardEnvVariables
        }
      ]
      scale: { minReplicas: 0, maxReplicas: 3 }
    }
  }
}

// Outputs to display your public entrance links
output apiRouterEastUsUrl string = 'https://${apiRouterEastUs.properties.configuration.ingress.fqdn}'
output apiRouterWestUsUrl string = 'https://${apiRouterWestUs.properties.configuration.ingress.fqdn}'
output apiRouterNorthEuropeUrl string = 'https://${apiRouterNorthEurope.properties.configuration.ingress.fqdn}'
