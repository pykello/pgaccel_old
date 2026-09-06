#pragma once
#include <string>
#include <string_view>
#include <vector>
namespace pgaccel::benchmark {
inline void AppendJsonString(std::string &output,std::string_view value) {
    constexpr char hex[]="0123456789abcdef";output+='"';
    size_t begin=0;
    for(size_t i=0;i<value.size();++i) {
        unsigned char c=value[i];if(c>=32 && c!='"' && c!='\\')continue;
        output.append(value.substr(begin,i-begin));begin=i+1;
        if(c=='"' || c=='\\'){output+='\\';output+=c;}
        else {output+="\\u00";output+=hex[c>>4];output+=hex[c&15];}
    }
    output.append(value.substr(begin));output+='"';
}
inline std::string JsonRows(const std::vector<std::vector<std::string>> &rows) {
    size_t estimate=2+3*rows.size();
    for(const auto &row:rows)for(const auto &value:row)estimate+=value.size()+3;
    std::string output;output.reserve(estimate);output+='[';
    for(size_t r=0;r<rows.size();++r) {
        if(r)output+=',';output+='[';
        for(size_t c=0;c<rows[r].size();++c){if(c)output+=',';AppendJsonString(output,rows[r][c]);}
        output+=']';
    }
    output+=']';return output;
}
}
