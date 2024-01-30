# Chartmogul Data Source Connector

ChartMogul is the leading subscription analytics platform to measure, understand, and grow recurring revenue of subscription businesses.
This component allows users to extract ChartMogul subscription data from the ChartMogul API. 

**Table of Contents**

[TOC]

## Configuration

### Authorization

To run this component, API keys are required. API keys must be `Active` with at least `Read` access level.

#### Obtaining API tokens
    Follow these steps in your ChartMogul web app:
      1. [Profile] - bottom left corner of the platform
      2. [Admin]
      3. Click on the user you wish to authorize as
      4. API Keys

### Row configuration
  - Endpoint - [REQ]
      1. `activities`
      2. `customers`
      3. `customers_subscriptions` - endpoint `customers` extraction will be inclusive with this endpoint
      4. `invoices`
      5. `key_metrics`
  - Incremental Load
  - Start date - [REQ for `key_metrics`][OPT for `activities`] 
      - Start date of the request. E.g., 2021-01-01, 1 day ago, 2 weeks ago
  - End date - [REQ for `key_metrics`][OPT for `activities`] 
      - End date of the request. E.g., 2021-01-01, 1 day ago, 2 weeks ago
      - If End Date is specified for `activities`, Start Date is also required.
  - Interval - [REQ for `key_metrics`]
      1. day
      2. week
      3. month
  - Geo [OPT for `key_metrics`]
      - A comma-separated list of ISO 3166-1 Alpha-2 formatted country codes to filter the results (e.g., US, GB, DE)
  - Plans [OPT for `key_metrics`]
      - A comma-separated list of plan names (as configured in your ChartMogul account) to filter the results. Note: Spaces must be URL-encoded and the names are case-sensitive (e.g., Silver%20plan,Gold%20plan,Enterprise%20plan).

  ### Sample configuration parameters
  ``` json
  {
    "parameters": {
        "#api_token": "123456789",
        "incrementalLoad": false,
        "endpoints": "key_metrics",
        "additional_params_activities": {
            "end-date": "",
            "start-date": ""
        },
        "additional_params_key_metrics": {
            "geo": "",
            "plans": "",
            "end-date": "2021-12-15",
            "interval": "week",
            "start-date": "2021-01-01"
        }
    }
  }
  ```

Supported endpoints
===================

If you need more endpoints, please submit your request to
[ideas.keboola.com](https://ideas.keboola.com/).


Development
===================

If required, change the local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the docker-compose file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace, and run the component with the following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone repo_path my-new-component
cd my-new-component
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with Keboola, please refer to the
[deployment section of our developer
documentation](https://developers.keboola.com/extend/component/deployment/).
