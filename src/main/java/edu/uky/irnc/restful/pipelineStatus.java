package edu.uky.irnc.restful;

import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class pipelineStatus {

    @SerializedName("pipelines")
    @Expose
    private List<Pipeline> pipelines = null;

    /**
     * No args constructor for use in serialization
     *
     */
    public pipelineStatus() {
    }

    /**
     *
     * @param pipelines
     */
    public pipelineStatus(List<Pipeline> pipelines) {
        super();
        this.pipelines = pipelines;
    }

    public List<Pipeline> getPipelines() {
        return pipelines;
    }

    public void setPipelines(List<Pipeline> pipelines) {
        this.pipelines = pipelines;
    }

    public pipelineStatus withPipelines(List<Pipeline> pipelines) {
        this.pipelines = pipelines;
        return this;
    }

}