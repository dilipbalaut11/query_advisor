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
PlannedStmt *pgqs_planner(Query *parse, const char *query_string,
						int cursorOptions, ParamListInfo boundParams);
#endif
