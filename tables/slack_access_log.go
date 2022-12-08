package tables

import (
	"context"
	"github.com/selefra/selefra-provider-slack/slack_client"

	"github.com/selefra/selefra-provider-sdk/provider/schema"
	"github.com/selefra/selefra-provider-sdk/provider/transformer/column_value_extractor"
	"github.com/selefra/selefra-provider-slack/table_schema_generator"
	"github.com/slack-go/slack"
)

type TableSlackAccessLogGenerator struct {
}

var _ table_schema_generator.TableSchemaGenerator = &TableSlackAccessLogGenerator{}

func (x *TableSlackAccessLogGenerator) GetTableName() string {
	return "slack_access_log"
}

func (x *TableSlackAccessLogGenerator) GetTableDescription() string {
	return ""
}

func (x *TableSlackAccessLogGenerator) GetVersion() uint64 {
	return 0
}

func (x *TableSlackAccessLogGenerator) GetOptions() *schema.TableOptions {
	return &schema.TableOptions{}
}

func (x *TableSlackAccessLogGenerator) GetDataSource() *schema.DataSource {
	return &schema.DataSource{
		Pull: func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, resultChannel chan<- any) *schema.Diagnostics {

			api, err := slack_client.Connect(ctx, taskClient.(*slack_client.Client).Config)

			if err != nil {
				return schema.NewDiagnosticsErrorPullTable(task.Table, err)
			}

			params := slack.AccessLogParameters{Count: 1000}
			maxPages := 5

			for params.Page <= maxPages {
				accessLogs, paging, err := api.GetAccessLogsContext(ctx, params)
				if err != nil {

					return schema.NewDiagnosticsErrorPullTable(task.Table, err)
				}
				for _, accessLog := range accessLogs {
					resultChannel <- accessLog

				}
				if paging.Page >= paging.Pages {
					break
				}
				params.Page = paging.Page + 1
			}
			return schema.NewDiagnosticsErrorPullTable(task.Table, nil)

		},
	}
}

func (x *TableSlackAccessLogGenerator) GetExpandClientTask() func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask) []*schema.ClientTaskContext {
	return nil
}

func (x *TableSlackAccessLogGenerator) GetColumns() []*schema.Column {
	return []*schema.Column{
		table_schema_generator.NewColumnBuilder().ColumnName("date_first").ColumnType(schema.ColumnTypeTimestamp).Description("Date of the first login in a sequence from this device.").
			Extractor(column_value_extractor.StructSelector("DateFirst")).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("isp").ColumnType(schema.ColumnTypeString).Description("ISP the login originated from, if available. Often null.").
			Extractor(column_value_extractor.StructSelector("ISP")).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("region").ColumnType(schema.ColumnTypeString).Description("Region the login originated from, if available. Often null.").Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("workspace_domain").ColumnType(schema.ColumnTypeString).Description("The domain name for the workspace.").
			Extractor(column_value_extractor.WrapperExtractFunction(func(ctx context.Context, clientMeta *schema.ClientMeta, taskClient any, task *schema.DataSourcePullTask, row *schema.Row, column *schema.Column, result any) (any, *schema.Diagnostics) {
				// 001
				r, err := slack_client.GetCommonColumns(ctx, clientMeta, taskClient, task, row, column, result)
				if err != nil {
					return nil, schema.NewDiagnosticsErrorColumnValueExtractor(task.Table, column, err)
				}
				extractor := column_value_extractor.StructSelector("Domain")
				return extractor.Extract(ctx, clientMeta, taskClient, task, row, column, r)
			})).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("user_id").ColumnType(schema.ColumnTypeString).Description("Unique identifier of the user").Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("user_name").ColumnType(schema.ColumnTypeString).Description("Name of the user.").
			Extractor(column_value_extractor.StructSelector("Username")).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("ip").ColumnType(schema.ColumnTypeString).Description("IP address the login came from.").
			Extractor(column_value_extractor.StructSelector("IP")).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("count").ColumnType(schema.ColumnTypeInt).Description("Number of sequential logins from this device.").Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("country").ColumnType(schema.ColumnTypeString).Description("Country the login originated from, if available. Often null.").Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("date_last").ColumnType(schema.ColumnTypeTimestamp).Description("Date of the last login in a sequence from this device.").
			Extractor(column_value_extractor.StructSelector("DateLast")).Build(),
		table_schema_generator.NewColumnBuilder().ColumnName("user_agent").ColumnType(schema.ColumnTypeString).Description("User agent of the device used for login.").Build(),
	}
}

func (x *TableSlackAccessLogGenerator) GetSubTables() []*schema.Table {
	return nil
}
