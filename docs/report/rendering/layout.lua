-- Keep figure captions with their images and leave space around the group.
local function latex(block)
  return pandoc.write(pandoc.Pandoc({block}), "latex"):gsub("%s+$", "")
end

-- Place the group note above the bold, evenly spaced author names.
function Meta(meta)
  if not FORMAT:match("latex") or not meta.author then
    return nil
  end
  local function metadata_latex(value)
    local blocks = value
    if pandoc.utils.type(value) ~= "Blocks" then
      blocks = {pandoc.Plain(value)}
    end
    return pandoc.write(pandoc.Pandoc(blocks), "latex"):gsub("%s+$", "")
  end
  local authors = meta.author
  if pandoc.utils.type(authors) ~= "List" then authors = {authors} end
  local names = {}
  for _, author in ipairs(authors) do
    names[#names + 1] = "\\textbf{" .. metadata_latex(author) .. "}"
  end
  local rendered = ""
  if meta.affiliation then
    rendered = "{\\normalsize " .. metadata_latex(meta.affiliation) .. "}"
  end
  if meta.thanks then
    rendered = rendered .. "\\thanks{\\protect\\parbox[t]{\\dimexpr\\linewidth-1.8em\\relax}{"
      .. metadata_latex(meta.thanks) .. "}}"
  end
  if meta.affiliation then rendered = rendered .. "\\\\[0.3em]" end
  rendered = rendered .. table.concat(names, "\\hspace{1.5em}")
  meta.author = pandoc.MetaInlines({pandoc.RawInline("latex", rendered)})
  meta.thanks = nil
  return meta
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
