/*
 * Copyright (c) 2020 MariaDB Corporation Ab
 * Copyright (c) 2023 MariaDB plc, Finnish Branch
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file and at www.mariadb.com/bsl11.
 *
 * Change Date: 2027-11-30
 *
 * On the date above, in accordance with the Business Source License, use
 * of this software will be governed by version 2 or later of the General
 * Public License.
 */
/**
 * Function to create chartjs tooltip element
 * @param {Object} payload.context - chartjs tooltip context
 * @param {String} payload.tooltipId - tooltipId. Use to remove the tooltip when chart instance is destroyed
 * @param {String} [payload.className] - Custom class name for tooltip element
 * @param {Boolean} payload.alignTooltipToLeft - To either align tooltip to the right or left.
 * If not provided, it aligns to center
 */
function createTooltipEle({ context, tooltipId, className, alignTooltipToLeft }) {
    let tooltipEl = document.getElementById(tooltipId)
    // Create element on first render
    if (!tooltipEl) {
        tooltipEl = document.createElement('div')
        tooltipEl.id = tooltipId
        tooltipEl.className = [`chartjs-tooltip shadow-drop ${className}`]
        tooltipEl.innerHTML = '<table></table>'
        // append to #app div so that vuetify class can be used
        document.getElementById('app').appendChild(tooltipEl)
    }
    const tooltip = context.tooltip
    // Hide if no tooltip
    if (tooltip.opacity === 0) {
        tooltipEl.style.opacity = 0
        return
    }

    // Set caret Position
    tooltipEl.classList.remove(
        'above',
        'below',
        'no-transform',
        'chartjs-tooltip--transform-left',
        'chartjs-tooltip--transform-right',
        'chartjs-tooltip--transform-center'
    )
    if (tooltip.yAlign) tooltipEl.classList.add(tooltip.yAlign)
    else tooltipEl.classList.add('no-transform')

    if (typeof alignTooltipToLeft === 'boolean') {
        if (alignTooltipToLeft) tooltipEl.classList.add('chartjs-tooltip--transform-left')
        else tooltipEl.classList.add('chartjs-tooltip--transform-right')
    } else tooltipEl.classList.add('chartjs-tooltip--transform-center')

    // Display, position, and set styles for font
    const position = context.chart.canvas.getBoundingClientRect()
    tooltipEl.style.opacity = 1
    tooltipEl.style.position = 'absolute'
    tooltipEl.style.left = `${position.left + tooltip.caretX}px`
    tooltipEl.style.top = `${position.top + tooltip.caretY + 10}px`
    tooltip.font = {
        family: "'azo-sans-web', adrianna, serif",
        size: 10,
    }
    return tooltipEl
}

/**
 * @param {string} borderColor
 * @returns {string}
 */
function genLegendTag(borderColor) {
    let style = 'background:' + borderColor
    style += '; border-color:' + borderColor
    style += '; border-width: 2px;margin-right:4px'
    return '<span class="chartjs-tooltip-key" style="' + style + '"></span>'
}

/**
 * Custom tooltip to show tooltip for multiple datasets. X axis value will be used
 * as the title of the tooltip. Body of the tooltip is generated by using
 * tooltipModel.body
 * @param {Object} payload.context - chartjs tooltip context
 * @param {String} payload.tooltipId - tooltipId. Use to remove the tooltip when chart instance is destroyed
 */
export function streamTooltip({ context, tooltipId, alignTooltipToLeft }) {
    // Tooltip Element

    let tooltipEl = createTooltipEle({ context, tooltipId, alignTooltipToLeft })
    const tooltip = context.tooltip
    // Set Text
    if (tooltipEl && tooltip.body) {
        let titleLines = tooltip.title || []
        let bodyLines = tooltip.body.map(item => item.lines)

        let innerHtml = '<thead>'

        titleLines.forEach(title => {
            innerHtml += '<tr><th>' + title + '</th></tr>'
        })
        innerHtml += '</thead><tbody>'

        bodyLines.forEach((body, i) => {
            innerHtml +=
                '<tr><td>' + genLegendTag(tooltip.labelColors[i].borderColor) + body + '</td></tr>'
        })
        innerHtml += '</tbody>'

        let tableRoot = tooltipEl.querySelector('table')
        tableRoot.innerHTML = innerHtml
    }
}

/**
 * Rendering object tooltip for a single dataset by using provided dataPointObj
 * @param {Object} payload.context - chartjs tooltip context
 * @param {String} payload.tooltipId - tooltipId. Use to remove the tooltip when chart instance is destroyed
 * @param {Object} payload.dataPoint - data point object
 * @param {String} payload.axisKeys.x - xAxisKey
 * @param {String} payload.axisKeys.y - yAxisKey
 */
export function objectTooltip({ context, tooltipId, dataPoint, axisKeys, alignTooltipToLeft }) {
    // Tooltip Element
    let tooltipEl = createTooltipEle({
        context,
        tooltipId,
        className: 'mxs-workspace-chart-tooltip',
        alignTooltipToLeft,
    })
    // Set Text
    if (tooltipEl && context.tooltip.body) {
        let innerHtml = '<tbody>'
        Object.keys(dataPoint).forEach(key => {
            //bold x,y axes value
            const boldClass = `${
                key === axisKeys.x || key === axisKeys.y ? 'font-weight-black' : ''
            }`
            innerHtml += `
                <tr>
                    <td class="mxs-color-helper text-small-text ${boldClass}">
                     ${key}
                     </td>
                    <td class="mxs-color-helper text-navigation ${boldClass}">
                        ${dataPoint[key]}
                    </td>
                </tr>`
        })
        innerHtml += '</tbody>'
        let tableRoot = tooltipEl.querySelector('table')
        tableRoot.innerHTML = innerHtml
    }
}

/**
 * Generate a tooltip for multiple datasets. Should be used when
 * tooltip mode is index
 * @param {object} param.
 * @param {object} param.context - chartjs tooltip context
 * @param {string} param.tooltipId - tooltipId. Use to remove the tooltip when chart instance is destroyed
 * @param {string} param.parsing.xAxisKey
 * @param {string} param.parsing.yAxisKey
 * @param {boolean} param.alignTooltipToLeft
 */
export function datasetObjectTooltip({ context, tooltipId, parsing, alignTooltipToLeft }) {
    // Tooltip Element
    let tooltipEl = createTooltipEle({
        context,
        tooltipId,
        className: 'mxs-workspace-chart-tooltip',
        alignTooltipToLeft,
    })
    const { tooltip: { dataPoints = [] } = {} } = context || {}
    const dataPointsLength = dataPoints.length
    if (tooltipEl && dataPointsLength) {
        let parts = []
        parts.push('<tbody>')
        dataPoints.forEach(({ dataset, dataIndex, datasetIndex }) => {
            const dataPoint = dataset.data[dataIndex]
            const dataPointKeys = Object.keys(dataPoint)
            const rowspan = dataPointKeys.length
            dataPointKeys.forEach((key, i) => {
                let cellClasses = `mxs-color-helper text-navigation`
                //bold x,y axes value
                if (key === parsing.xAxisKey || key === parsing.yAxisKey)
                    cellClasses += ' font-weight-black'
                if (
                    dataPointsLength > 1 &&
                    datasetIndex < dataPointsLength - 1 &&
                    i === rowspan - 1
                )
                    cellClasses += ' pb-3'

                parts.push('<tr>')

                if (i === 0)
                    parts.push(
                        `<td rowspan="${rowspan}" style="vertical-align: middle;">
                    ${genLegendTag(dataset.borderColor)}
                     ${dataset.label}
                   </td>
                    `
                    )

                parts.push(`<td class="${cellClasses}">${key}</td>`)
                parts.push(`<td class="${cellClasses}">${dataPoint[key]}</td>`)
                parts.push('</tr>')
            })
        })

        parts.push('</tbody>')
        let tableRoot = tooltipEl.querySelector('table')
        tableRoot.innerHTML = parts.join('\n')
    }
}
