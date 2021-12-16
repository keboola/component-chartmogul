# Chartmogul Extractor

ChartMogul is the leading subscription analytics platform to measure, undertsand, and grow their recurring revenue.
This component allows user to extract ChartMogul subscription data from the ChartMogul API. 

**Table of Contents**

[TOC]

## Configuration

### Authorization

To run this component, API keys are required. API keys are required to be `Active` and contains `Read` as a minimum access level.

#### Obtaining API Tokens
    Follow path below:
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
      - Start date of the request. Eg: 2021-01-01, 1 day ago, 2 weeks ago
  - End date - [REQ for `key_metrics`][OPT for `activities`] 
      - End date of the request. Eg: 2021-01-01, 1 day ago, 2 weeks ago
      - If End Date is specified for `activities`, Start Date will be required.
  - Interval - [REQ for `key_metrics`]
      1. day
      2. week
      3. month
  - Geo [OPT for `key_metrics`]
      - A comma-separated list of ISO 3166-1 Alpha-2 formatted country codes to filter the results to, e.g. US,GB,DE
  - Plans [OPT for `key_metrics`]
      - A comma-separated list of plan names (as configured in your ChartMogul account) to filter the results to. Note that spaces must be url-encoded and the names are case-sensitive, e.g. Silver%20plan,Gold%20plan,Enterprise%20plan.

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
[ideas.keboola.com](https://ideas.keboola.com/)


Development
===================

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the docker-compose file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
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

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)
