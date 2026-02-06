# MQTT to Azure Event Hubs (OAuth) - Home Assistant add-on

Forwards MQTT messages to Azure Event Hubs using Entra ID OAuth (Client ID/Secret).

## Azure setup (required)
1. Create App Registration (Service Principal).
2. Create a Client Secret.
3. Assign RBAC on Event Hubs namespace or the specific Event Hub:
   - Azure Event Hubs Data Sender

## Add-on options
- eventhub_fqdn: <namespace>.servicebus.windows.net
- eventhub_name: your-eventhub
- tenant_id, client_id, client_secret: from Entra ID app registration
- mqtt_host: core-mosquitto (if using HA Mosquitto add-on)
- mqtt_topic: home/# (filter)

## Fabric
Use Fabric Eventstream -> Source: Azure Event Hubs -> Destination: KQL DB or Lakehouse.