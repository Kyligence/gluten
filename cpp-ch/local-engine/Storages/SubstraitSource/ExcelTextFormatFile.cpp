/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ExcelTextFormatFile.h"


#include <memory>
#include <string>
#include <utility>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeDecimalBase.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/Serializations/SerializationNullable.h>
#include <Formats/FormatSettings.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/SeekableReadBuffer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Storages/HDFS/ReadBufferFromHDFS.h>
#include <Storages/Serializations/ExcelDecimalSerialization.h>
#include <Storages/Serializations/ExcelSerialization.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
}
}

namespace local_engine
{

void skipErrorChars(DB::ReadBuffer & buf, bool has_quote, char maybe_quote, const DB::FormatSettings & settings)
{
    char skip_before_char = has_quote ? maybe_quote : settings.csv.delimiter;

    /// skip all chars before quote/delimiter exclude line delimiter
    while (!buf.eof() && *buf.position() != skip_before_char && *buf.position() != '\n' && *buf.position() != '\r')
        ++buf.position();

    /// if char is quote, skip it
    if (has_quote && !buf.eof() && *buf.position() == maybe_quote)
        ++buf.position();
}

FormatFile::InputFormatPtr ExcelTextFormatFile::createInputFormat(const DB::Block & header)
{
    auto res = std::make_shared<FormatFile::InputFormat>();
    res->read_buffer = read_buffer_builder->build(file_info, true);

    DB::FormatSettings format_settings = createFormatSettings();
    size_t max_block_size = file_info.text().max_block_size();
    DB::RowInputFormatParams params = {.max_block_size = max_block_size};

    std::shared_ptr<DB::PeekableReadBuffer> buffer = std::make_unique<DB::PeekableReadBuffer>(*(res->read_buffer));
    DB::Names column_names;
    column_names.reserve(file_info.schema().names_size());
    for (const auto & item : file_info.schema().names())
    {
        column_names.push_back(item);
    }

    std::shared_ptr<local_engine::ExcelRowInputFormat> txt_input_format = std::make_shared<local_engine::ExcelRowInputFormat>(
        header, buffer, params, format_settings, column_names, file_info.text().escape());
    res->input = txt_input_format;
    return res;
}

DB::FormatSettings ExcelTextFormatFile::createFormatSettings()
{
    DB::FormatSettings format_settings = DB::getFormatSettings(context);
    format_settings.csv.trim_whitespaces = true;
    format_settings.with_names_use_header = true;
    format_settings.with_types_use_header = false;
    format_settings.skip_unknown_fields = true;
    std::string delimiter = file_info.text().field_delimiter();
    format_settings.csv.delimiter = *delimiter.data();

    if (file_info.start() == 0)
        format_settings.csv.skip_first_lines = file_info.text().header();

    if (delimiter == "\t" || delimiter == " ")
        format_settings.csv.allow_whitespace_or_tab_as_delimiter = true;

    if (!file_info.text().null_value().empty())
        format_settings.csv.null_representation = file_info.text().null_value();

    if (format_settings.csv.null_representation.empty())
        format_settings.csv.empty_as_default = true;
    else
        format_settings.csv.empty_as_default = false;

    char quote = *file_info.text().quote().data();
    if (quote == '\'')
    {
        format_settings.csv.allow_single_quotes = true;
        format_settings.csv.allow_double_quotes = false;
    }
    else
    {
        /// quote == '"' and default
        format_settings.csv.allow_single_quotes = false;
        format_settings.csv.allow_double_quotes = true;
    }

    return format_settings;
}


ExcelRowInputFormat::ExcelRowInputFormat(
    const DB::Block & header_,
    std::shared_ptr<DB::PeekableReadBuffer> & buf_,
    const DB::RowInputFormatParams & params_,
    const DB::FormatSettings & format_settings_,
    DB::Names & input_field_names_,
    String escape_)
    : CSVRowInputFormat(
        header_,
        buf_,
        params_,
        true,
        false,
        format_settings_,
        std::make_unique<ExcelTextFormatReader>(*buf_, input_field_names_, format_settings_))
    , escape(escape_)
{
    DB::Serializations gluten_serializations;
    for (const auto & item : data_types)
    {
        const DataTypePtr nest_type = item->isNullable() ? static_cast<const DataTypeNullable &>(*item).getNestedType() : item;
        SerializationPtr nest_serialization;
        WhichDataType which(nest_type->getTypeId());
        if (which.isDecimal32())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal32> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal32>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal64())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal64> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal64>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal128())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal128> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal128>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else if (which.isDecimal256())
        {
            const auto & decimal_type = static_cast<const DataTypeDecimalBase<Decimal256> &>(*nest_type);
            nest_serialization = std::make_shared<ExcelDecimalSerialization<Decimal256>>(
                nest_type->getDefaultSerialization(), decimal_type.getPrecision(), decimal_type.getScale());
        }
        else
            nest_serialization = std::make_shared<ExcelSerialization>(nest_type->getDefaultSerialization(), escape);


        if (item->isNullable())
            gluten_serializations.insert(gluten_serializations.end(), std::make_shared<SerializationNullable>(nest_serialization));
        else
            gluten_serializations.insert(gluten_serializations.end(), nest_serialization);
    }

    serializations = gluten_serializations;
}


ExcelTextFormatReader::ExcelTextFormatReader(
    DB::PeekableReadBuffer & buf_, DB::Names & input_field_names_, const DB::FormatSettings & format_settings_)
    : CSVFormatReader(buf_, format_settings_), input_field_names(input_field_names_)
{
}


std::vector<String> ExcelTextFormatReader::readNames()
{
    return input_field_names;
}

std::vector<String> ExcelTextFormatReader::readTypes()
{
    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED, "ExcelTextRowInputFormat::readTypes is not implemented");
}

bool ExcelTextFormatReader::readField(
    DB::IColumn & column,
    const DB::DataTypePtr & type,
    const DB::SerializationPtr & serialization,
    bool is_last_file_column,
    const String &)
{
    if (isEndOfLine())
    {
        column.insertDefault();
        return false;
    }

    preSkipNullValue();
    size_t column_size = column.size();

    if (format_settings.csv.trim_whitespaces && isNumber(removeNullable(type)))
        skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    const bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
    const bool at_last_column_line_end = is_last_file_column && (buf->eof() || *buf->position() == '\n' || *buf->position() == '\r');

    /// Note: Tuples are serialized in CSV as separate columns, but with empty_as_default or null_as_default
    /// only one empty or NULL column will be expected
    if (format_settings.csv.empty_as_default && (at_delimiter || at_last_column_line_end))
    {
        /// Treat empty unquoted column value as default value, if
        /// specified in the settings. Tuple columns might seem
        /// problematic, because they are never quoted but still contain
        /// commas, which might be also used as delimiters. However,
        /// they do not contain empty unquoted fields, so this check
        /// works for tuples as well.
        column.insertDefault();
        return false;
    }

    char maybe_quote = *buf->position();
    bool has_quote = false;
    if ((format_settings.csv.allow_single_quotes && maybe_quote == '\'')
        || (format_settings.csv.allow_double_quotes && maybe_quote == '\"'))
        has_quote = true;

    auto column_back_func = [&column_size](DB::IColumn & column_back) -> void
    {
        if (column_back.isNullable())
        {
            ColumnNullable & col = assert_cast<ColumnNullable &>(column_back);
            if (col.getNullMapData().size() == column_size + 1)
                col.getNullMapData().pop_back();
            if (col.getNestedColumn().size() == column_size + 1)
                col.getNestedColumn().popBack(1);
        }
    };

    try
    {
        /// Read the column normally.
        serialization->deserializeTextCSV(column, *buf, format_settings);
    }
    catch (Exception & e)
    {
        /// Logic for possible skipping of errors.
        if (!isParseError(e.code()))
            throw;

        skipErrorChars(*buf, has_quote, maybe_quote, format_settings);
        column_back_func(column);
        column.insertDefault();

        return false;
    }

    if (column_size == column.size())
    {
        skipErrorChars(*buf, has_quote, maybe_quote, format_settings);
        column_back_func(column);
        column.insertDefault();
        return false;
    }

    return true;
}

void ExcelTextFormatReader::preSkipNullValue()
{
    /// null_representation is empty and value is "" or '' in spark return null
    if(((format_settings.csv.allow_single_quotes && *buf->position() == '\'')
            || (format_settings.csv.allow_double_quotes && *buf->position() == '\"')))
    {
        PeekableReadBufferCheckpoint checkpoint{*buf, false};
        char maybe_quote = *buf->position();
        ++buf->position();

        if (!buf->eof() && *buf->position() == maybe_quote)
            ++buf->position();
        else
        {
            buf->rollbackToCheckpoint();
            return;
        }

        bool at_delimiter = !buf->eof() && *buf->position() == format_settings.csv.delimiter;
        bool at_line_end = buf->eof() || *buf->position() == '\n' || *buf->position() == '\r';

        if (!at_delimiter && !at_line_end)
            buf->rollbackToCheckpoint();
    }
}

void ExcelTextFormatReader::skipFieldDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (!isEndOfLine())
        assertChar(format_settings.csv.delimiter, *buf);
}

bool ExcelTextFormatReader::isEndOfLine()
{
    return buf->eof() || *buf->position() == '\r' || *buf->position() == '\n';
}


void ExcelTextFormatReader::skipRowEndDelimiter()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);

    if (buf->eof())
        return;

    /// we support the extra delimiter at the end of the line
    if (*buf->position() == format_settings.csv.delimiter)
        ++buf->position();

    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    if (buf->eof())
        return;

    if (!isEndOfLine())
    {
        // remove unused chars
        skipField();
        skipRowEndDelimiter();
    }
    else
        skipEndOfLine(*buf);
}

void ExcelTextFormatReader::skipField()
{
    skipWhitespacesAndTabs(*buf, format_settings.csv.allow_whitespace_or_tab_as_delimiter);
    NullOutput out;
    readCSVStringIntoPro(out, *buf, format_settings.csv);
}

template<typename T>
concept WithResize = requires (T value)
{
    { value.resize(1) };
    { value.size() } -> std::integral<>;
};

template <typename T>
static void appendToStringOrVector(T & s, ReadBuffer & rb, const char * end)
{
    s.append(rb.position(), end - rb.position());
}

template <typename Vector, bool include_quotes>
void ExcelTextFormatReader::readCSVStringIntoPro(Vector & s, ReadBuffer & buf, const FormatSettings::CSV & settings)
{
    /// Empty string
    if (buf.eof())
        return;

    const char delimiter = settings.delimiter;
    const char maybe_quote = *buf.position();
    const String & custom_delimiter = settings.custom_delimiter;

    /// Emptiness and not even in quotation marks.
    if (custom_delimiter.empty() && maybe_quote == delimiter)
        return;

    if ((settings.allow_single_quotes && maybe_quote == '\'') || (settings.allow_double_quotes && maybe_quote == '"'))
    {
        if constexpr (include_quotes)
            s.push_back(maybe_quote);

        ++buf.position();

        /// The quoted case. We are looking for the next quotation mark.
        while (!buf.eof())
        {

            char * next_pos = reinterpret_cast<char *>(memchr(buf.position(), maybe_quote, buf.buffer().end() - buf.position()));

            if (*(next_pos-1) =='\\')
            {
                buf.position() = next_pos+1;
                continue;
            }


            if (nullptr == next_pos)
                next_pos = buf.buffer().end();

            appendToStringOrVector(s, buf, next_pos);

            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            if constexpr (include_quotes)
                s.push_back(maybe_quote);

            /// Now there is a quotation mark under the cursor. Is there any following?
            ++buf.position();

            if (buf.eof())
                return;

            if (*buf.position() == maybe_quote)
            {
                s.push_back(maybe_quote);
                ++buf.position();
                continue;
            }

            return;
        }
    }
    else
    {
        /// If custom_delimiter is specified, we should read until first occurrences of
        /// custom_delimiter in buffer.
        if (!custom_delimiter.empty())
        {
            PeekableReadBuffer * peekable_buf = dynamic_cast<PeekableReadBuffer *>(&buf);
            if (!peekable_buf)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading CSV string with custom delimiter is allowed only when using PeekableReadBuffer");

            while (true)
            {
                if (peekable_buf->eof())
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Unexpected EOF while reading CSV string, expected custom delimiter \"{}\"", custom_delimiter);

                char * next_pos = reinterpret_cast<char *>(memchr(peekable_buf->position(), custom_delimiter[0], peekable_buf->available()));
                if (!next_pos)
                    next_pos = peekable_buf->buffer().end();

                appendToStringOrVector(s, *peekable_buf, next_pos);
                peekable_buf->position() = next_pos;

                if (!buf.hasPendingData())
                    continue;

                {
                    PeekableReadBufferCheckpoint checkpoint{*peekable_buf, true};
                    if (checkString(custom_delimiter, *peekable_buf))
                        return;
                }

                s.push_back(*peekable_buf->position());
                ++peekable_buf->position();
            }

            return;
        }

        /// Unquoted case. Look for delimiter or \r or \n.
        while (!buf.eof())
        {
            char * next_pos = buf.position();

            [&]()
            {
#ifdef __SSE2__
                auto rc = _mm_set1_epi8('\r');
                auto nc = _mm_set1_epi8('\n');
                auto dc = _mm_set1_epi8(delimiter);
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    __m128i bytes = _mm_loadu_si128(reinterpret_cast<const __m128i *>(next_pos));
                    auto eq = _mm_or_si128(_mm_or_si128(_mm_cmpeq_epi8(bytes, rc), _mm_cmpeq_epi8(bytes, nc)), _mm_cmpeq_epi8(bytes, dc));
                    uint16_t bit_mask = _mm_movemask_epi8(eq);
                    if (bit_mask)
                    {
                        next_pos += std::countr_zero(bit_mask);
                        return;
                    }
                }
#elif defined(__aarch64__) && defined(__ARM_NEON)
                auto rc = vdupq_n_u8('\r');
                auto nc = vdupq_n_u8('\n');
                auto dc = vdupq_n_u8(delimiter);
                for (; next_pos + 15 < buf.buffer().end(); next_pos += 16)
                {
                    uint8x16_t bytes = vld1q_u8(reinterpret_cast<const uint8_t *>(next_pos));
                    auto eq = vorrq_u8(vorrq_u8(vceqq_u8(bytes, rc), vceqq_u8(bytes, nc)), vceqq_u8(bytes, dc));
                    uint64_t bit_mask = getNibbleMask(eq);
                    if (bit_mask)
                    {
                        next_pos += std::countr_zero(bit_mask) >> 2;
                        return;
                    }
                }
#endif
                while (next_pos < buf.buffer().end()
                       && *next_pos != delimiter && *next_pos != '\r' && *next_pos != '\n')
                    ++next_pos;
            }();

            appendToStringOrVector(s, buf, next_pos);
            buf.position() = next_pos;

            if (!buf.hasPendingData())
                continue;

            if constexpr (WithResize<Vector>)
            {
                if (settings.trim_whitespaces) [[likely]]
                {
                    /** CSV format can contain insignificant spaces and tabs.
                    * Usually the task of skipping them is for the calling code.
                    * But in this case, it will be difficult to do this, so remove the trailing whitespace by ourself.
                    */
                    size_t size = s.size();
                    while (size > 0 && (s[size - 1] == ' ' || s[size - 1] == '\t'))
                        --size;

                    s.resize(size);
                }
            }
            return;
        }
    }
}


void ExcelTextFormatReader::skipEndOfLine(DB::ReadBuffer & in)
{
    /// \n (Unix) or \r\n (DOS/Windows) or \n\r (Mac OS Classic)

    if (*in.position() == '\n')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\r')
            ++in.position();
    }
    else if (*in.position() == '\r')
    {
        ++in.position();
        if (!in.eof() && *in.position() == '\n')
            ++in.position();
        /// Different with CH master:
        /// removed \r check
    }
    else if (!in.eof())
        throw DB::Exception(DB::ErrorCodes::INCORRECT_DATA, "Expected end of line");
}

inline void ExcelTextFormatReader::skipWhitespacesAndTabs(ReadBuffer & in, bool allow_whitespace_or_tab_as_delimiter)
{
    if (allow_whitespace_or_tab_as_delimiter)
    {
        return;
    }
    /// Skip `whitespace` symbols allowed in CSV.
    while (!in.eof() && (*in.position() == ' ' || *in.position() == '\t'))
        ++in.position();
}


}
