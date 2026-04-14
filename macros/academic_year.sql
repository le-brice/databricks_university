{% macro academic_year(date_column) %}
    {#
        Returns the Dutch/TU/e academic year for a given date.
        Academic year starts in August: AY 2024 = Aug 2024 – Jul 2025.
        E.g. 2024-09-01 → 2024, 2025-03-01 → 2024, 2025-08-01 → 2025
    #}
    case
        when month({{ date_column }}) >= {{ var('academic_year_start_month', 8) }}
        then year({{ date_column }})
        else year({{ date_column }}) - 1
    end
{% endmacro %}
