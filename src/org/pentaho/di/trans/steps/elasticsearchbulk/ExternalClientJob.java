package org.pentaho.di.trans.steps.elasticsearchbulk;

import org.elasticsearch.client.Client;

public interface ExternalClientJob {

    public Client getClient();

}