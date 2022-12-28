#pragma once

#include "column_data.hpp"
#include "types.hpp"
#include <vector>
#include <string>

namespace pgaccel
{

int CountMatches(const std::vector<ColumnDataP>& columnDataVec, 
                 const std::string &valueStr,
                 const pgaccel::AccelType *type,
                 bool useAvx);

};
