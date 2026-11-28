CREATE TABLE [dbo].[PipelineControl] (
    [ControlId]        BIGINT        NOT NULL,
    [FabricPipelineId] VARCHAR (200) NOT NULL,
    [SourceSystem]     VARCHAR (50)  NOT NULL,
    [SourceSchema]     VARCHAR (128) NOT NULL,
    [SourceTable]      VARCHAR (128) NOT NULL,
    [TargetTable]      VARCHAR (128) NOT NULL,
    [LoadType]         VARCHAR (20)  NOT NULL,
    [WatermarkColumn]  VARCHAR (128) NULL,
    [LastWatermark]    DATETIME2 (3) NULL,
    [IsActive]         BIT           NOT NULL,
    [LoadOrder]        INT           NOT NULL
);


GO