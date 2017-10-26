package edu.uky.irnc.restful;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class pipelineStatus implements Serializable
{

    @SerializedName("pipelines")
    @Expose
    private List<Pipeline> pipelines = new ArrayList<Pipeline>();
    private final static long serialVersionUID = 898847269549373332L;

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