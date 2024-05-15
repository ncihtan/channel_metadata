SELECT
  Channel_Metadata_ID,
  ARRAY_AGG(
    CASE 
      WHEN attribute.attributeName IN ('Antibody Name', 'Antibody name') AND attribute.attributeValue IS NOT NULL 
      THEN attribute.attributeValue 
    END
  IGNORE NULLS) AS Antibody_Name,
  ARRAY_AGG(
    CASE 
      WHEN attribute.attributeName IN ('Channel Name', 'Channel', 'CHANNEL', 'channel', 'channelName') OR attribute.attributeName LIKE 'BEMS%' 
      AND attribute.attributeValue IS NOT NULL 
      THEN attribute.attributeValue 
    END
  IGNORE NULLS) AS Channel_Name,
  ARRAY_AGG(
    CASE 
      WHEN attribute.attributeName IN ('MARKERNAME', 'Markers', 'marker_name') AND attribute.attributeValue IS NOT NULL 
      THEN attribute.attributeValue 
    END
  IGNORE NULLS) AS Marker_Name
FROM
  `htan-dcc.ISB_CGC_r5.channel_metadata`,
  UNNEST(channel_attributes) AS attribute
WHERE
  Imaging_Assay_Type != 'H&E'
  AND Imaging_Assay_Type != 'MERFISH'
GROUP BY
  Channel_Metadata_ID