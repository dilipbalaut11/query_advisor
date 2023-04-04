/*-------------------------------------------------------------------------
 *
 * generate_advise.h: 
 *
 *
 *-------------------------------------------------------------------------
*/
#ifndef _GENERATE_ADVISE_H_
#define _GENERATE_ADVISE_H_

extern planner_hook_type prev_planner_hook;
extern bool pgqs_cost_track_enable;
extern PlannedStmt *pgqs_planner(Query *parse,
#if PG_VERSION_NUM >= 130000
			         const char *query_string,
#endif
				 int cursorOptions,
				 ParamListInfo boundParams);
#endif
