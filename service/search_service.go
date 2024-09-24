package service

import (
	"access-service/entity"
	"access-service/model"
	"context"
	"github.com/dapr-platform/common"
	"github.com/pkg/errors"
	"strconv"
	"strings"
)

func QueryAllTagKey(ctx context.Context, relType string) (datas []string, err error) {
	qstr := "_select=" + model.Tag_FIELD_NAME_key + "&_distinct=true&rel_type=" + relType
	keys, err := common.DbQuery[map[string]any](ctx, common.GetDaprClient(), model.TagTableInfo.Name, qstr)
	if err != nil {
		err = errors.Wrap(err, "db query error")
		return
	}
	for _, v := range keys {
		datas = append(datas, v[model.Tag_FIELD_NAME_key].(string))
	}
	return
}

func QueryAllTagValue(ctx context.Context, tag string) (datas []string, err error) {
	qstr := "_select=" + model.Tag_FIELD_NAME_value + "&_distinct=true&" + model.Tag_FIELD_NAME_key + "=" + tag
	values, err := common.DbQuery[map[string]any](ctx, common.GetDaprClient(), model.TagTableInfo.Name, qstr)
	if err != nil {
		err = errors.Wrap(err, "db query error")
		return
	}
	for _, v := range values {
		datas = append(datas, v[model.Tag_FIELD_NAME_value].(string))
	}
	return
}
func QueryPointNamesByTags(ctx context.Context, tags string) (names []string, err error) {
	selectStr := "distinct name"
	fromStr := "v_point_with_tag"
	whereStr := ""
	if tags != "" {
		arr := strings.Split(tags, ",")
		for _, s := range arr {
			if s != "" {
				whereStr += " '" + s + "'=any(tags)" + " and"
			}

		}
	}
	if whereStr != "" {
		whereStr = whereStr[:strings.LastIndex(whereStr, " and")]
	} else {
		whereStr = "1=1"
	}
	data, err := common.CustomSql[map[string]string](ctx, common.GetDaprClient(), selectStr, fromStr, whereStr)
	if err != nil {
		err = errors.Wrap(err, "select data error")
		return
	}
	for _, d := range data {
		names = append(names, d["name"])
	}
	return
}
func QueryDeviceByTags(ctx context.Context, page, pageSize int, tags string) (pageData common.PageGeneric[entity.DeviceCurrentData], err error) {
	selectStr := "*"
	countSelectStr := "count(*)"
	fromStr := "v_device_current_data"
	whereStr := ""
	if tags != "" {
		arr := strings.Split(tags, ",")
		for _, s := range arr {
			if s != "" {
				whereStr += " '" + s + "'=any(tags)" + " and"
			}

		}
	}
	if whereStr != "" {
		whereStr = whereStr[:strings.LastIndex(whereStr, " and")]
	} else {
		whereStr = "1=1"
	}
	countWhereStr := whereStr
	whereStr += " limit " + strconv.Itoa(pageSize) + " offset " + strconv.Itoa((page-1)*pageSize)
	counts, err := common.CustomSql[entity.CountVo](ctx, common.GetDaprClient(), countSelectStr, fromStr, countWhereStr)
	if err != nil {
		err = errors.Wrap(err, "select count error")
		return
	}
	if len(counts) != 1 {
		err = errors.New("select count error,no data")
		return
	}
	count := counts[0].Count

	data, err := common.CustomSql[entity.DeviceCurrentData](ctx, common.GetDaprClient(), selectStr, fromStr, whereStr)
	if err != nil {
		err = errors.Wrap(err, "select data error")
		return
	}
	pageData = common.PageGeneric[entity.DeviceCurrentData]{
		Page:     page,
		PageSize: pageSize,
		Total:    count,
		Items:    data,
	}
	return
}

func QueryDeviceByTagValueLike(ctx context.Context, page, pageSize int, queryStr string) (pageData common.PageGeneric[entity.DeviceCurrentData], err error) {
	selectStr := "v.*"
	countSelectStr := "count(v.*)"
	fromStr := ` v_device_current_data v,(
SELECT distinct o_device.id
FROM o_device
INNER JOIN o_point ON o_device.id = o_point.device_id
INNER JOIN o_tag ON (o_device.id = o_tag.rel_id) `
	if queryStr != "" {
		fromStr += " WHERE "
		arr := strings.Split(queryStr, " ")
		orArr := make([]string, 0)
		for _, s := range arr {
			if s != "" {
				orArr = append(orArr, " o_tag.value LIKE '%"+s+"%' ")
			}
		}
		fromStr += strings.Join(orArr, " OR ")
		fromStr += ") b"
	} else {
		fromStr += `) b `
	}
	whereStr := "v.id=b.id "

	countWhereStr := whereStr
	whereStr += " limit " + strconv.Itoa(pageSize) + " offset " + strconv.Itoa((page-1)*pageSize)
	counts, err := common.CustomSql[entity.CountVo](ctx, common.GetDaprClient(), countSelectStr, fromStr, countWhereStr)
	if err != nil {
		err = errors.Wrap(err, "select count error")
		return
	}
	if len(counts) != 1 {
		err = errors.New("select count error,no data")
		return
	}
	count := counts[0].Count

	data, err := common.CustomSql[entity.DeviceCurrentData](ctx, common.GetDaprClient(), selectStr, fromStr, whereStr)
	if err != nil {
		err = errors.Wrap(err, "select data error")
		return
	}
	pageData = common.PageGeneric[entity.DeviceCurrentData]{
		Page:     page,
		PageSize: pageSize,
		Total:    count,
		Items:    data,
	}
	return
}
