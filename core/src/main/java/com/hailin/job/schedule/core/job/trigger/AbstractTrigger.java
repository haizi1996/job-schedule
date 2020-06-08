package com.hailin.job.schedule.core.job.trigger;

import com.hailin.job.schedule.core.basic.AbstractElasticJob;
import com.hailin.shrine.job.common.util.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTrigger implements Trigger{

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTrigger.class);

    protected AbstractElasticJob job;

    @Override
    public void init(AbstractElasticJob job) {
        this.job = job;
    }

    @Override
    public Triggered createTriggered(boolean yes, String upStreamDataStr) {
        Triggered triggered = new Triggered();
        triggered.setYes(yes);
        triggered.setUpStreamData(JsonUtils.fromJson(upStreamDataStr, TriggeredData.class));
        triggered.setDownStreamData(new TriggeredData());
        return triggered;
    }

    @Override
    public String serializeDownStreamData(Triggered triggered) {
        return JsonUtils.toJson(triggered.getDownStreamData());
    }
}
