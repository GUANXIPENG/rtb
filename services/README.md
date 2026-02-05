必填字段：
`id`（string）：请求 ID
`imp[]`（array）：至少一个广告位
`imp[].id`（string）
`imp[].bidfloor`（number，可选）
`imp[].ext.placement_id`（int）：本 DSP 必填

用于定向的可选字段：
`user.id` / `user.buyeruid`
`user.geo`（country/region/city/metro/zip）
`user.interests`（字符串数组）
`device.geo`（同上）
`device.os` / `device.osv` / `device.make` / `device.model`
