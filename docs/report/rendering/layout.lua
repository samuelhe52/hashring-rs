-- Keep figure captions with their images and leave space around the group.
local function latex(block)
  return pandoc.write(pandoc.Pandoc({block}), "latex"):gsub("%s+$", "")
end

function Figure(figure)
  if not FORMAT:match("latex") then return nil end
  if #figure.content ~= 1 or figure.content[1].t ~= "Plain"
      or #figure.content[1].content ~= 1 or figure.content[1].content[1].t ~= "Image" then
    error("Report figures must contain one standalone image")
  end
  figure.content[1].content[1].caption = {}
  local image = latex(figure.content[1])
  local caption = pandoc.write(pandoc.Pandoc(figure.caption.long), "latex")
  return pandoc.RawBlock("latex", table.concat({
    "\\begin{center}",
    "\\begin{minipage}{\\linewidth}",
    "\\vspace*{8pt}",
    "\\centering",
    image .. "\\par\\medskip",
    "{\\small " .. caption .. "\\par}",
    "\\vspace{8pt}",
    "\\end{minipage}",
    "\\end{center}"
  }, "\n"))
end

function Table(table)
  if not FORMAT:match("latex") then return nil end
  -- Permit long PascalCase identifiers to wrap at word boundaries in cells.
  table = table:walk({Code = function(code)
    local rendered = pandoc.write(pandoc.Pandoc({pandoc.Plain({code})}), "latex")
    rendered = rendered:gsub("([a-z])([A-Z])", "%1\\allowbreak{}%2")
    return pandoc.RawInline("latex", rendered:gsub("%s+$", ""))
  end})
  return pandoc.RawBlock("latex", "\\Needspace{8\\baselineskip}\n" .. latex(table))
end

function Header(header)
  if not FORMAT:match("latex") then return nil end
  for _, class in ipairs(header.classes) do
    if class == "page-break" then
      return {pandoc.RawBlock("latex", "\\clearpage"), header}
    end
  end
end
