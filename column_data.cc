#include "column_data.hpp"

namespace pgaccel
{

template<>
Result<bool>
DictColumnData<StringType>::SaveValue(std::ostream &out,
                                      const std::string &value) const
{
    int len = value.length();
    out.write((char *) &len, sizeof(len));
    out.write(value.c_str(), len);
    return true;
}

};
