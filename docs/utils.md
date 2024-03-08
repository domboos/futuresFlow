## Utiltiy Functions Description:

### create_ret_mat:
takes ascending returns as an input and returns a matrix where the rows are the desecending returns.

Example:

| time | Price | ret  |
|------|-------|------|
| t_0  | P_0   | NA   |
| t_1  | P_1   | r_1  |
| t_2  | P_2   | r_2  |
| t_3  | P_3   | r_3  |
| t_4  | P_4   | r_4  |
| ...  | ...   | ...  |
| t_10 | P_10  | r_10 |

with maxlag = 3 returns

| index   | ret0 | ret1  | ret2  | ret3  | ret4  |
|---------|------|-------|-------|-------|-------|
| t_4=0   | r_4  | r_3   | r_2   | r_1   | r_0   |
| t_5=1   | r_5  | r_4   | r_3   | r_2   | r_1   |
| t_6=2   | r_6  | r_5   | r_4   | r_3   | r_2   |
| t_7=3   | r_7  | r_6   | r_5   | r_4   | r_3   |
| ..      | ..   | ..    | ..    | ..    | ..    |
| t_N=N-4 | r_N  | r_N-1 | r_N-2 | r_N-3 | r_N-4 |

