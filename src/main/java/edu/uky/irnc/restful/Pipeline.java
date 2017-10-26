package edu.uky.irnc.restful;
import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Pipeline {

    @SerializedName("tenant_id")
    @Expose
    private String tenantId;
    @SerializedName("status_code")
    @Expose
    private String statusCode;
    @SerializedName("status_desc")
    @Expose
    private String statusDesc;
    @SerializedName("pipeline_id")
    @Expose
    private String pipelineId;
    @SerializedName("pipeline_name")
    @Expose
    private String pipelineName;

    /**
     * No args constructor for use in serialization
     *
     */
    public Pipeline() {
    }

    /**
     *
     * @param statusCode
     * @param tenantId
     * @param pipelineId
     * @param pipelineName
     * @param statusDesc
     */
    public Pipeline(String tenantId, String statusCode, String statusDesc, String pipelineId, String pipelineName) {
        super();
        this.tenantId = tenantId;
        this.statusCode = statusCode;
        this.statusDesc = statusDesc;
        this.pipelineId = pipelineId;
        this.pipelineName = pipelineName;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public Pipeline withTenantId(String tenantId) {
        this.tenantId = tenantId;
        return this;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }

    public Pipeline withStatusCode(String statusCode) {
        this.statusCode = statusCode;
        return this;
    }

    public String getStatusDesc() {
        return statusDesc;
    }

    public void setStatusDesc(String statusDesc) {
        this.statusDesc = statusDesc;
    }

    public Pipeline withStatusDesc(String statusDesc) {
        this.statusDesc = statusDesc;
        return this;
    }

    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public Pipeline withPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
        return this;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public Pipeline withPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
        return this;
    }

}